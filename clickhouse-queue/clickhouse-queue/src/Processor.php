<?php
namespace REES46\ClickHouse;

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

	/**
	 * @var Cli
	 */
	private $cli;

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
		$this->cli = new Cli($this->config['clickhouse'], $this->logger);

		//Ожидаем очередь
		while(count($this->channel->callbacks)) {
			$this->channel->wait();
		}
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

			//Отправляем данные
			$this->cli->insert($body['table'], $body['values']);

			//Дополнительные алгоритмы при вставке в таблицу
			try {
				$this->tableProcessor($body);
			} catch(\Exception $e) {
				$this->logger->error($e->getMessage() . $e->getTraceAsString());
			}

			//Отвечаем, что успешно
			$this->channel->basic_ack($tag);
		} catch (ProcessorException $e) {
			$this->logger->error($e->getMessage());
			sleep(1);
			$this->channel->basic_reject($tag, true);
		} catch (\Exception $e) {
			$this->logger->error($e->getMessage() . $e->getTraceAsString());
			sleep(1);
			$this->channel->basic_reject($tag, true);
		}
	}

	/**
	 * Обработчик
	 * @param array $body
	 */
	protected function tableProcessor($body) {
		switch( $body['table'] ) {

			//Таблица событий
			case 'actions':
				$this->actionsProcessor($body);
				break;

			//Таблица заказов
			case 'order_items':
				$this->orderItemProcessor($body);
				break;
		}
	}

	/**
	 * Обработчик таблицы заказа
	 * @param array $body
	 */
	protected function orderItemProcessor($body) {

		//Если дополнительные параметры не указаны, выходим
		if( empty($body['opts']) ) {
			return;
		}

		//Дата выборки
		$date = date('Y-m-d', strtotime('-2 DAYS'));

		//Получаем список компаний
		$campaigns = $this->cli->get("SELECT object_id, brand FROM actions WHERE session_id = {$body['values']['session_id']}
 																				AND shop_id = {$body['values']['shop_id']}
 																				AND event = 'recone_click'
 																				AND object_type = 'VendorCampaign'
 																				AND date >= '{$date}'");

		//Если нашлись компании вендоров
		if( !empty($campaigns) ) {
			foreach($campaigns as $campaign) {

				//Если бренды совпадают
				if( mb_strtolower($campaign->brand) == mb_strtolower($body['values']['brand']) ) {
					$this->cli->insert('actions', [
						'session_id'           => $body['values']['session_id'],
						'current_session_code' => $body['opts']['current_session_code'] ?? '',
						'shop_id'              => $body['values']['shop_id'],
						'event'                => 'recone_purchase',
						'object_type'          => 'VendorCampaign',
						'object_id'            => $campaign->object_id,
						'price'                => $body['values']['price'] * $body['values']['amount'],
						'recommended_by'       => $body['values']['recommended_by'] ?? null,
						'referer'              => $body['opts']['referer'] ?? '',
						'useragent'            => $body['opts']['useragent'] ?? '',
					]);
				}
			}
		}
	}

	/**
	 * Обработчик таблицы событий
	 * @param $body
	 */
	protected function actionsProcessor($body) {

		//Если было событие просмотра товара и указан, что пришел из рекомендера
		//Пробуем найти событие recone_view которое добавляется при генерации рекомендации
		//Если событие просмотра было найдено и нашлась кампания, добавляем, что клиент кликнул по нашему баннеру
		if( $body['values']['event'] == 'view' && $body['values']['recommended_by'] ) {

			//Дата выборки
			$date = date('Y-m-d', strtotime('-2 HOUR'));
			$dateTime = date('Y-m-d H:i:s', strtotime('-2 HOUR'));

			//Получаем последнюю
			$campaigns = $this->cli->get("SELECT object_id, brand FROM actions WHERE session_id = {$body['values']['session_id']} 
																					AND shop_id = {$body['values']['shop_id']}
																					AND event = 'recone_view'
																					AND object_type = 'VendorCampaign'
																					AND recommended_by = '{$body['values']['recommended_by']}'
																					AND brand = '{$body['values']['brand']}'
																					AND date >= '{$date}'
																					AND created_at >= '{$dateTime}'
																					LIMIT 1");

			//Если нашлись компании вендоров
			if( !empty($campaigns) ) {
				foreach($campaigns as $campaign) {
					$this->cli->insert('actions', [
						'session_id'           => $body['values']['session_id'],
						'current_session_code' => $body['opts']['current_session_code'] ?? '',
						'shop_id'              => $body['values']['shop_id'],
						'event'                => 'recone_click',
						'object_type'          => 'VendorCampaign',
						'object_id'            => $campaign->object_id,
						'price'                => $body['values']['price'] * $body['values']['amount'],
						'recommended_by'       => $body['values']['recommended_by'],
						'referer'              => $body['opts']['referer'] ?? '',
						'useragent'            => $body['opts']['useragent'] ?? '',
					]);
				}
			}
		}
	}
}