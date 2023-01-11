<?php
namespace REES46\ClickHouse;

use Amp\Socket\ConnectException;
use League\CLImate\CLImate;
use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Exception\ConnectionException;
use PHPinnacle\Ridge\Message;
use REES46\Core\Clickhouse;
use REES46\RabbitMQ\RabbitMQ;
use REES46\Error\ClickhouseException;
use REES46\Core\Logger;
use REES46\Worker\BaseConsumeWorker;
use Revolt\EventLoop;
use function Amp\async;
use function Amp\delay;

/**
 * ClickHouse Daemon
 * @package REES46\ClickHouse
 */
class Processor extends BaseConsumeWorker {

	private array $queue_time = [];
	private bool $started = false;
	private string $loop_queue;
	private string $loop_bad;
	private bool $lock = false;

	/**
	 * API WebServer constructor.
	 * @param CLImate $cli
	 */
	public function __construct(CLImate $cli) {
		parent::__construct($cli, CONFIG, 'Queue worker', 'rees46-clickhouse-queue');
	}

	/**
	 * @inheritDoc
	 */
	public function initializeConnections(): void {

		//Устанавливаем при подключении
		RabbitMQ::get()->pool = 500;
	}

	public function onReload(): void {
		EventLoop::cancel($this->loop_queue);
		EventLoop::cancel($this->loop_bad);
		parent::onReload();
	}

	/**
	 * @throws ClickhouseException
	 */
	public function onStarted(): void {
		Clickhouse::$timeout = 60;
		Clickhouse::$inactive_timeout = 60;
		Clickhouse::$transfer_timeout = 60;
		Logger::$logger->info('Starting');

		//Получаем список всех таблиц кликхауса
		$tables = array_filter(array_column(Clickhouse::get()->execute('SHOW TABLES'), 'name'), fn($v) => !str_starts_with($v, '.'));
		foreach( $tables as $table ) {
			Clickhouse::get()->schema($table);
		}

		//Функция обработки сломанных файлов
		$bad = function() {
			if( !$this->lock ) {
				try {
					$this->lock = true;
					$this->queueUpdated(true);
				} finally {
					$this->lock = false;
				}
			}
		};

		//Сначала обрабатываем очередь
		$this->queueUpdated();
		async($bad);
		Logger::$logger->info('Queue cleared');
		$this->started = true;
		delay(1);

		//Подписываемся на канал
		RabbitMQ::get()->channel->queueDeclare(RabbitMQ::CLICKHOUSE_QUEUE, false, true);

		//Подписываем колбек функцию
		RabbitMQ::get()->consume([$this, 'received'], RabbitMQ::CLICKHOUSE_QUEUE, false, null, true);

		//Запускаем обработку очередей каждые 20 секунд
		$this->loop_queue = EventLoop::repeat(20, fn() => $this->queueUpdated());
		//Раз в час проверяем сломанные файлы
		$this->loop_bad = EventLoop::repeat(3600, $bad);
	}

	/**
	 * Получили сообщение
	 * @param Message $message
	 * @param Channel $channel
	 */
	public function received(Message $message, Channel $channel): void {
		try {
			$this->task_working++;

			//Новый вариант, когда таблица отправляется в заголовках.
			//При этом хорошо бы, чтоб формат данных был корректный, тогда не придется декодировать и кодировать json. Т.к. Clickhouse не дает вставить число в строковую колонку.
			if( $message->header('table') ) {
				$table = $message->header('table');
				$values = json_decode($message->content, true);
			} else {
				//Старый вариант вставки
				$body = json_decode($message->content, true);
				$table = $body['table'];
				$values = $body['values'];
			}

			//Форматируем вставляемые данные
			foreach( $values as $column => $value ) {
				$values[$column] = Clickhouse::get()->convertToType($table, $column, $value);
			}

			//Добавляем строку в файл вставки
			$path = $this->dumpPath($table);
			file_put_contents($path, json_encode($values, JSON_UNESCAPED_UNICODE) . PHP_EOL, FILE_APPEND);

			//Запоминаем время вставки
			if( !isset($this->queue_time[$table]) ) {
				$this->queue_time[$table] = time();
			}

			//Подтверждаем прием
			$channel->ack($message);

			//Если набрали данных на пачку
			if( $this->availableForProcessing($table) ) {
				$this->queueProcessing($path, $table);
			}
		} catch (ConnectionException|ConnectException $e) {
			delay(10);
			$channel->reject($message);
		} catch (\Throwable $e) {
			Logger::$logger->error(get_class($e) . ', ' . $e->getMessage() . ', table: ' . $table . ', message: ' . json_encode($values, JSON_UNESCAPED_UNICODE) . PHP_EOL . $e->getTraceAsString());
			delay(10);
			try {
				$channel->reject($message);
			} catch (\Throwable $e) {
				Logger::$logger->warning($e->getMessage());
			}
		} finally {
			$this->task_working--;
		}
	}

	/**
	 * Возвращает путь к файлу для вставки
	 * @param string $table
	 * @return string
	 */
	protected function dumpPath(string $table) {
		return APP_ROOT . '/tmp/' . $table . '.json';
	}

	/**
	 * Обрабатывает внутреннюю очередь, чтобы вставить данные пачкой
	 * @param bool $force Форсированная обработка упавших файлов
	 */
	protected function queueUpdated(bool $force = false): void {
		//Проходим по локальной очереди
		foreach( glob(pathinfo($this->dumpPath('1'), PATHINFO_DIRNAME) . '/*.json' . ($force ? '.*' : '')) as $filename ) {
			$table = pathinfo(preg_replace('/\.json(\..*?)?$/', '.json', $filename), PATHINFO_FILENAME);
			if( $force || $this->availableForProcessing($table) ) {
				$this->queueProcessing($filename, $table);
			}
		}
	}

	/**
	 * Проверяет, готова ли очередь для вставки в таблицу
	 * @param $table
	 * @return bool
	 */
	protected function availableForProcessing($table): bool {
		if( !$this->started ) {
			return true;
		}

		if( !isset($this->queue_time[$table]) ) {
			return false;
		}

		//Если не было вставки и последний раз данные приходили больше 20 секунд назад
		if( isset($this->queue_time[$table]) && $this->queue_time[$table] <= strtotime('-20 seconds') ) {
			return true;
		}

		//Размер больше 1 Мб, вставляем
		return filesize($this->dumpPath($table)) >= 1048576;
	}

	/**
	 * Запускает обработку очереди для указанной таблицы
	 * @param string $file
	 * @param string $table
	 */
	protected function queueProcessing(string $file, string $table): void {
		//Копируем данные, чтобы во время обработки их не дополнили случайно
		if( str_ends_with($file, '.json') ) {
			$path = $file . '.' . uniqid();
			rename($file, $path);
			unset($this->queue_time[$table]);
		} else {
			$path = $file;
		}

		try {
			$data = file_get_contents($path);
			Clickhouse::get()->bulkDataInsertJson($data, $table);
			unlink($path);
		} catch (\Exception $e) {
			Logger::$logger->error(get_class($e) . ', ' . $e->getMessage(), array_slice($e->getTrace(), 0, 2));
		}
	}
}
