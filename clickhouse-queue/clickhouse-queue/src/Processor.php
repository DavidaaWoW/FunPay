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
	 * @var GeoDetector
	 */
	private $geo_detector;

	/**
	 * Run daemon
	 * @param Logger $logger
	 * @param array $config
	 */
	public function __construct(Logger $logger, $config) {

		//Читаем конфиги
		$this->config = $config;
		$this->logger = $logger;

		//Инициализируем определение местоположения
		$this->geo_detector = new GeoDetector($config['geo']);

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
			$body = $this->beforeTableProcessor(json_decode($body, true));

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
	 * Дополняет данные при вставке в таблицу
	 * @param array $body
	 * @return array
	 */
	protected function beforeTableProcessor($body) {
		switch( $body['table'] ) {

			//Дополняем данными о местоположении по ip
			case 'visits':
				if( !empty($body['values']['ip']) ) {
					$body['values'] = array_merge($body['values'], $this->geo_detector->detect($body['values']['ip']));
				}
				break;
		}
		return $body;
	}

	/**
	 * Обработчик
	 * @param array $body
	 */
	protected function tableProcessor($body) {
		switch( $body['table'] ) {

			//Таблица событий
			case 'actions':
			case 'actions_sharded':
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

		//Если дополнительные параметры или бренд не указаны, выходим
		if( empty($body['opts']) || empty($body['values']['brand']) ) {
			return;
		}

		//Дата выборки
		$date = date('Y-m-d', strtotime('-2 DAYS'));

		//Получаем список компаний
		$campaigns = $this->cli->get("SELECT object_id, brand FROM recone_actions WHERE session_id = {$body['values']['session_id']}
 																				AND shop_id = {$body['values']['shop_id']}
 																				AND event = 'click'
 																				AND item_id = '{$body['values']['item_uniqid']}'
 																				AND object_type = 'VendorCampaign'
 																				AND brand = '" . addslashes($body['values']['brand']) . "'
 																				AND date >= '{$date}'");

		//Если нашлись компании вендоров
		if( !empty($campaigns) ) {
			foreach($campaigns as $campaign) {

				//Если бренды совпадают
				if( mb_strtolower($campaign->brand) == mb_strtolower($body['values']['brand']) ) {
					$this->cli->insert('recone_actions', [
						'session_id'           => $body['values']['session_id'],
						'current_session_code' => $body['opts']['current_session_code'] ?? '',
						'shop_id'              => $body['values']['shop_id'],
						'event'                => 'purchase',
						'item_id'              => $body['values']['item_uniqid'],
						'object_type'          => 'VendorCampaign',
						'object_id'            => $campaign->object_id,
						'price'                => $body['values']['price'],
						'amount'               => $body['values']['amount'] ?? 1,
						'brand'                => $body['values']['brand'],
						'recommended_by'       => $body['values']['recommended_by'] ?? null,
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

		//Если было событие просмотра товара и указан, что пришел из рекомендера и у товара есть бренд
		//Пробуем найти событие recone_view которое добавляется при генерации рекомендации
		//Если событие просмотра было найдено и нашлась кампания, добавляем, что клиент кликнул по нашему баннеру
		if( $body['values']['event'] == 'view' && $body['values']['object_type'] == 'Item' && $body['values']['recommended_by'] && $body['values']['brand'] ) {

			//Дата выборки
			$date = date('Y-m-d', strtotime('-2 HOUR'));
			$dateTime = date('Y-m-d H:i:s', strtotime('-2 HOUR'));

			//Получаем последнюю. Если будут проблемы с простановкой флага recommended_by при просмотре товара, просто убрать фильтрацию и брать recommended_by из события.
			$campaigns = $this->cli->get("SELECT object_id, object_price, brand FROM recone_actions WHERE session_id = {$body['values']['session_id']} 
																					AND shop_id = {$body['values']['shop_id']}
																					AND event = 'view'
																					AND item_id = '{$body['values']['object_id']}'
																					AND object_type = 'VendorCampaign'
																					AND recommended_by = '{$body['values']['recommended_by']}'
																					AND brand = '" . addslashes($body['values']['brand']) . "'
																					AND date >= '{$date}'
																					AND created_at >= '{$dateTime}'
																					ORDER BY created_at DESC
																					LIMIT 1");

			//Если нашлись компании вендоров
			if( !empty($campaigns) ) {
				foreach($campaigns as $campaign) {

					//Если бренды совпадают
					if( mb_strtolower($campaign->brand) == mb_strtolower($body['values']['brand']) ) {
						$this->cli->insert('recone_actions', [
							'session_id'           => $body['values']['session_id'],
							'current_session_code' => $body['values']['current_session_code'] ?? '',
							'shop_id'              => $body['values']['shop_id'],
							'event'                => 'click',
							'item_id'              => $body['values']['object_id'],
							'object_type'          => 'VendorCampaign',
							'object_id'            => $campaign->object_id,
							'object_price'         => $campaign->object_price,
							'price'                => $body['values']['price'],
							'brand'                => $body['values']['brand'],
							'recommended_by'       => $body['values']['recommended_by'],
						]);
					}
				}
			}
		}
	}
}