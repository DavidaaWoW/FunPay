<?php
namespace REES46\ClickHouse;

use Amp\Socket\ConnectException;
use League\CLImate\CLImate;
use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Exception\ConnectionException;
use PHPinnacle\Ridge\Message;
use REES46\Core\Clickhouse;
use REES46\Core\SpaceMessage;
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
	private array $lock = [true => false, false => false];
	private array $fp = [];

	/**
	 * API WebServer constructor.
	 * @param CLImate $cli
	 */
	public function __construct(CLImate $cli) {
		parent::__construct($cli, CONFIG, 'Clickhouse Queue Worker', 'rees46-clickhouse-queue');
	}

	/**
	 * @inheritDoc
	 */
	public function initializeConnections(): void {

		//Устанавливаем при подключении
		RabbitMQ::get()->pool = 10;
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
		Clickhouse::$timeout = 120;
		Clickhouse::$inactive_timeout = 120;
		Clickhouse::$transfer_timeout = 120;
		Logger::$logger->info('Starting');
		Logger::$logger->debug('class: ' . get_class(\Amp\File\createDefaultDriver()));

		//Получаем список всех таблиц кликхауса
		$tables = array_filter(array_column(Clickhouse::get()->execute('SHOW TABLES'), 'name'), fn($v) => !str_starts_with($v, '.'));
		foreach( $tables as $table ) {
			Clickhouse::get()->schema($table);
		}

		//Сначала обрабатываем очередь
		$this->queueUpdated();
		async(fn() => $this->queueUpdated(true));
		Logger::$logger->info('Queue cleared');
		$this->started = true;

		//Подписываемся на канал
		RabbitMQ::get()->channel->queueDeclare(RabbitMQ::CLICKHOUSE_QUEUE, false, true);

		//Подписываем колбек функцию
		RabbitMQ::get()->consume([$this, 'received'], RabbitMQ::CLICKHOUSE_QUEUE, false, null, true);

		//Запускаем обработку очередей каждые 20 секунд
		$this->loop_queue = EventLoop::repeat(20, fn() => $this->queueUpdated());
		//Раз в час проверяем сломанные файлы
		$this->loop_bad = EventLoop::repeat(3600, fn() => $this->queueUpdated(true));
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
			if( empty($this->fp[$table]) ) {
				$this->fp[$table] = fopen($this->dumpPath($table), 'a+');
				stream_set_blocking($this->fp[$table], false);
			}
			//Вызываем блокировку файла и пишем в него
			if( flock($this->fp[$table], LOCK_EX) ) {
				fwrite($this->fp[$table], json_encode($values, JSON_UNESCAPED_UNICODE) . PHP_EOL);
			}
			flock($this->fp[$table], LOCK_UN);

			//Запоминаем время вставки
			if( !isset($this->queue_time[$table]) ) {
				$this->queue_time[$table] = time();
			}

			//Подтверждаем прием
			$channel->ack($message);
		} catch (ConnectionException|ConnectException $e) {
			delay(10);
			$channel->reject($message);
		} catch (\Throwable $e) {
			Logger::$logger->error(get_class($e) . ', ' . $e->getMessage() . ', table: ' . $table . ', message: ' . json_encode($values, JSON_UNESCAPED_UNICODE) . PHP_EOL . $e->getTraceAsString());
			SpaceMessage::throw($this->name, $e, ['method' => 'Processor::received', 'line' => __LINE__, 'table' => $table, 'values' => $values]);
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
	public function queueUpdated(bool $force = false): void {
		if( !$this->lock[$force] ) {
			try {
				$this->lock[$force] = true;
				//Проходим по локальной очереди
				foreach( glob(pathinfo($this->dumpPath('1'), PATHINFO_DIRNAME) . '/*.json' . ($force ? '.*' : '')) as $filename ) {
					$table = pathinfo(preg_replace('/\.json(\..*?)?$/', '.json', $filename), PATHINFO_FILENAME);
					if( $force || $this->availableForProcessing($table) ) {
						$this->queueProcessing($filename, $table);
						if( $force ) {
							delay(1);
						}
					}
				}
			} finally {
				$this->lock[$force] = false;
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

		//Размер больше 5 Мб, вставляем
		return filesize($this->dumpPath($table)) >= 5242880;
	}

	/**
	 * Запускает обработку очереди для указанной таблицы
	 * @param string $file
	 * @param string $table
	 */
	protected function queueProcessing(string $file, string $table): void {
		//Копируем данные, чтобы во время обработки их не дополнили случайно
		if( str_ends_with($file, '.json') ) {

			//Закрываем файл и удаляем из массива
			if( isset($this->fp[$table]) && flock($this->fp[$table], LOCK_EX) ) {
				fclose($this->fp[$table]);
				unset($this->fp[$table]);
			}

			//Переименовываем файл
			$path = $file . '.' . uniqid();
			rename($file, $path);
			unset($this->queue_time[$table]);
		} else {
			$path = $file;
		}

		try {
			$this->task_working++;
			$fp = fopen($path, 'r');
			stream_set_blocking($fp, false);
			$data = stream_get_contents($fp);
			fclose($fp);
			Clickhouse::get()->bulkDataInsertJson($data, $table);
			unlink($path);
		} catch (\Exception $e) {
			Logger::$logger->error(get_class($e) . ', ' . $e->getMessage(), array_slice($e->getTrace(), 0, 2));
		} finally {
			$this->task_working--;
		}
	}
}
