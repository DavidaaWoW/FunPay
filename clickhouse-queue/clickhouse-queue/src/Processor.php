<?php
namespace REES46\ClickHouse;

use League\CLImate\CLImate;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;

/**
 * ClickHouse Daemon
 * @package REES46\ClickHouse
 */
class Processor {

	/**
	 * @var AMQPStreamConnection
	 */
	private $connection;

	/**
	 * @var AMQPChannel
	 */
	private $channel;

	/**
	 * Connection config
	 * @var array
	 */
	private $config;

	private $ch;

	/**
	 * @var Logger
	 */
	private $logger;

	/**
	 * Run daemon
	 * @param Logger $logger
	 * @param array $config
	 */
	public function __construct(Logger $logger, $config) {

		//Читаем конфиги
		$this->config = $config;
		$this->logger = $logger;

		//Подключаемся к RabbitMQ
		$this->connection = new AMQPStreamConnection($this->config['rabbit']['host'], $this->config['rabbit']['port'], $this->config['rabbit']['user'], $this->config['rabbit']['password']);
		$this->channel = $this->connection->channel();

		//Подписываемся на канал
		$this->channel->queue_declare('clickhouse', false, true, false, false);

		//Устанавливаем что мы работаем в несколько потоков
		$this->channel->basic_qos(null, 1, null);

		//Подписываем колбек функцию
		$this->channel->basic_consume('clickhouse', getmypid(), false, false, false, false, function($message) {
			$this->received($message->body, $message->delivery_info['delivery_tag']);
		});

		//Подключаемся к кликхаусу
		$this->cluckHouseConnect();

		//Ожидаем очередь
		while(count($this->channel->callbacks)) {
			$this->channel->wait();
		}
	}

	//Подключаемся к кликхаусу
	public function cluckHouseConnect() {
		$this->ch = curl_init();
		curl_setopt($this->ch, CURLOPT_URL, "http://{$this->config['clickhouse']['host']}:{$this->config['clickhouse']['port']}/?database={$this->config['clickhouse']['database']}");
		curl_setopt($this->ch, CURLOPT_POST, 1);
		curl_setopt($this->ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($this->ch, CURLOPT_FOLLOWLOCATION, true);
	}

	/**
	 * Получили сообщение
	 * @param string $body
	 * @param integer $tag
	 * @throws \Exception
	 */
	public function received($body, $tag) {
		try {
			//Делаем работу
			$body = json_decode($body, true);

			//Строим строку вставки
			$sql = sprintf('INSERT INTO %s (%s) VALUES (%s)', $body['table'], implode(', ', array_keys($body['values'])), implode(', ', array_map([$this, 'quote'], $body['values'])));
			$this->logger->debug('SQL: ' . $sql);

			//Отправляем данные
			curl_setopt($this->ch, CURLOPT_POSTFIELDS, $sql);
			$response = curl_exec($this->ch);
			$code = curl_getinfo($this->ch, CURLINFO_HTTP_CODE);

			if( $code >= 400 || $code < 200 ) {
				throw new ProcessorException('[CODE: ' . $code . ']' . $response);
			}

			//Отвечаем, что успешно
			$this->channel->basic_ack($tag);
		} catch (ProcessorException $e) {
			$this->logger->error($e->getMessage());
			sleep(1);
			$this->channel->basic_reject($tag, true);
		} catch (\Exception $e) {
			sleep(1);
			$this->channel->basic_reject($tag, true);
			throw $e;
		}
	}

	/**
	 * @param {string|int|null} $data
	 * @return string
	 * @throws \Exception
	 */
	protected function quote($data) {
		if( is_float($data) || is_int($data) ) {
			return $data;
		} else {
			if( is_string($data) ) {
				return '\'' . addslashes($data) . '\'';
			} else {
				if( $data ) {
					throw new \Exception('Invalid data type.');
				} else {
					return 'NULL';
				}
			}
		}
	}
}