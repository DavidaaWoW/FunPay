<?php
namespace REES46\ClickHouse;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use REQRequest\Rabbitmq;

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

	private $batch_size;
	private $batch = [];
	private $queue = [];

	/**
	 * Run daemon
	 * @param Logger $logger
	 * @param array $config
	 */
	public function __construct(Logger $logger, $config) {
		date_default_timezone_set('UTC');

		//Читаем конфиги
		$this->config = $config;
		$this->logger = $logger;

		//Проверяем, чтобы максимальное число в пачке для таблиц было меньше или равно общей
		$this->batch_size = $config['rabbit']['batch_size'] ?? 1;
		$this->batch = $config['rabbit']['batch'] ?? [];
		if( !empty($this->batch) && $this->batch_size < max($this->batch) ) {
			$logger->error('Variable batch_size is less a max value batch');
			while(1) {
				sleep(100);
			}
		}

		//Инициализируем определение местоположения
		$this->geo_detector = new GeoDetector($config['geo']);

		//Подключаемся к RabbitMQ
		$this->connection = new AMQPStreamConnection($this->config['rabbit']['host'], $this->config['rabbit']['port'], $this->config['rabbit']['user'], $this->config['rabbit']['password']);
		$this->channel = $this->connection->channel();

		//Подписываемся на канал
		$this->channel->queue_declare('clickhouse', false, true, false, false);

		//Устанавливаем что мы работаем в несколько потоков
		$this->channel->basic_qos(null, $this->batch_size + array_sum($this->batch), true);

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

			//Проверяем очередь
			if( !isset($this->queue[$body['table']]) ) {
				$this->queue[$body['table']] = [
					'time'  => time(),
					'queue' => [],
				];
			}

			//Добавляем в очередь
			$this->queue[$body['table']]['queue'][$tag] = $body;

			//Вызываем триггер на обработку массива очереди
			$this->queueUpdated();

		} catch (\Exception $e) {
			$this->logger->error($e->getMessage() . $e->getTraceAsString());
			sleep(2);
			$this->channel->basic_reject($tag, true);
			unset($this->queue[$body['table']]['queue'][$tag]);
		}
	}

	/**
	 * Обрабатывает внутреннюю очередь чтобы вставить данные пачкой
	 */
	protected function queueUpdated() {

		//Проходим по внутренней очереди
		foreach( $this->queue as $table => $data ) {

			//Если наступило время обработки таблицы
			if( $this->availableForProcessing($table, $data['time']) ) {
				$this->queueProcessing($table);
			}
		}
	}

	/**
	 * Проверяет, готова ли очередь для вставки в таблицу
	 * @param $table
	 * @param $created_at
	 * @return bool
	 */
	protected function availableForProcessing($table, $created_at) {

		//Если время последней вставки больше 10 секунд, сразу отправляем пачку
		if( in_array($table, ['device_events', 'push_attributes']) ) {
			if( $created_at < strtotime('-30 seconds') ) {
				return true;
			}
		} elseif( $created_at < strtotime('-10 seconds') ) {
			return true;
		}

		//Получаем количество данных в очереди для вставки
		$count = count($this->queue[$table]['queue']);

		//Если текущая таблица присутствует в списке
		if( isset($this->batch[$table]) ) {
			return $count >= $this->batch[$table];
		}

		//Общие условия для вставки
		return $count > $this->batch_size / count($this->queue[$table]) * 0.25;
	}

	/**
	 * Запускает обработку очереди для указанной таблицы
	 * @param $table
	 */
	protected function queueProcessing($table) {
		//Копируем данные, чтобы во время обработки их не дополнили случайно
		$bulk = (new \ArrayObject($this->queue[$table]['queue']))->getArrayCopy();
		unset($this->queue[$table]);

		try {
			$this->cli->bulkInsert($table, $bulk);

			//Отвечаем, что успешно
			foreach( array_keys($bulk) as $tag ) {
				$this->channel->basic_ack($tag);
			}
		} catch (ProcessorException $e) {
			$this->logger->error($e->getMessage());
			sleep(1);
			foreach( array_keys($bulk) as $tag ) {
				$this->channel->basic_reject($tag, true);
			}
		} catch (\Exception $e) {
			$this->logger->error($e->getMessage() . $e->getTraceAsString());
			sleep(10);
			foreach( array_keys($bulk) as $tag ) {
				$this->channel->basic_reject($tag, true);
			}
		}

		//Дополнительные алгоритмы при вставке в таблицу
		foreach( $bulk as $body ) {
			try {
				$this->tableProcessor($body);
			} catch(\Exception $e) {
				$this->logger->error($e->getMessage() . $e->getTraceAsString());
			}
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
	 * @throws ProcessorException
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
	 * @throws ProcessorException
	 */
	protected function orderItemProcessor($body) {

		//Если дополнительные параметры или бренд не указаны, выходим
		if( empty($body['opts']) || empty($body['values']['brand']) ) {
			return;
		}

		//Дата выборки
		$date = date('Y-m-d', strtotime('-2 DAYS'));

		//Получаем список компаний
		$campaigns = $this->cli->get("SELECT DISTINCT object_id, brand FROM recone_actions WHERE did = {$this->cli->quote($body['values']['did'])}
 																				AND shop_id = {$body['values']['shop_id']}
 																				AND event = 'click'
 																				AND item_id = '{$body['values']['item_uniqid']}'
 																				AND object_type = 'VendorCampaign'
 																				AND date >= '{$date}'");

		//Если нашлись компании вендоров
		if( !empty($campaigns) ) {
			foreach($campaigns as $campaign) {

				//Добавляем событие в очередь
				$this->channel->basic_publish(new AMQPMessage(json_encode([
					'table' => 'recone_actions',
					'values' => [
						'did'            => $body['values']['did'],
						'sid'            => $body['opts']['current_session_code'] ?? '',
						'shop_id'        => $body['values']['shop_id'],
						'event'          => 'purchase',
						'item_id'        => $body['values']['item_uniqid'],
						'object_type'    => 'VendorCampaign',
						'object_id'      => $campaign->object_id,
						'object_price'   => 0,
						'price'          => $body['values']['price'],
						'amount'         => $body['values']['amount'] ?? 1,
						'brand'          => $body['values']['brand'],
						'recommended_by' => $body['values']['recommended_by'] ?? null,
						'referer'        => null,
						'position'       => $body['values']['position'] ?? null,
					],
					'opts' => [],
				])), '', 'clickhouse');
			}
		}
	}

	/**
	 * Обработчик таблицы событий
	 * @param $body
	 * @throws ProcessorException
	 */
	protected function actionsProcessor($body) {

		//Дата выборки
		$date = date('Y-m-d', strtotime('-2 HOUR'));
		$dateTime = date('Y-m-d H:i:s', strtotime('-2 HOUR'));
		$dateHour = date('Y-m-d', strtotime('-1 HOUR'));
		$dateTimeHour = date('Y-m-d H:i:s', strtotime('-1 HOUR'));

		//Если было событие просмотра товара и указан, что пришел из рекомендера и у товара есть бренд
		//Пробуем найти событие recone_view которое добавляется при генерации рекомендации
		//Если событие просмотра было найдено и нашлась кампания, добавляем, что клиент кликнул по нашему баннеру
		//---
		//Для события корзины или покупки, проверяем, если не было события клика, а сразу добавлен в корзину или куплен в один клик, добавляем клик.
		if( in_array($body['values']['event'], ['view', 'cart', 'purchase']) && $body['values']['object_type'] == 'Item' && $body['values']['recommended_by'] && $body['values']['brand'] ) {

			//Получаем последнюю. Если будут проблемы с простановкой флага recommended_by при просмотре товара, просто убрать фильтрацию и брать recommended_by из события.
			$campaigns = $this->cli->get("SELECT DISTINCT object_id, object_price, brand FROM recone_actions WHERE did = {$this->cli->quote($body['values']['did'])} 
																					AND shop_id = {$body['values']['shop_id']}
																					AND event = 'view'
																					AND item_id = '{$body['values']['object_id']}'
																					AND object_type = 'VendorCampaign'
																					AND recommended_by = '{$body['values']['recommended_by']}'
																					AND date >= '{$date}'
																					AND created_at >= '{$dateTime}'
																					ORDER BY created_at DESC");

			//Если нашлись компании вендоров
			if( !empty($campaigns) ) {
				foreach($campaigns as $campaign) {

					//Если событие просмотра товара, то разрешаем добавлять клик
					$access = $body['values']['event'] == 'view';

					//Проверяем, чтобы не было повторного клика в течении часа только для текущей кампании
					if( $body['values']['event'] == 'view' ) {
						$actions = $this->cli->get("SELECT 1 FROM recone_actions WHERE
																							did = {$this->cli->quote($body['values']['did'])} 
																							AND shop_id = {$body['values']['shop_id']}
																							AND event = 'click'
																							AND object_type = 'VendorCampaign'
																							AND object_id = '{$campaign->object_id}'
																							AND date >= '{$dateHour}'
																							AND created_at >= '{$dateTimeHour}'
																							ORDER BY created_at DESC");
						//Если вернулся пустой результат, добавляем событие клика
						$access = empty($actions);
					}

					//Если событие корзины или покупки и клика не было
					if( in_array($body['values']['event'], ['cart', 'purchase']) && $access ) {

						try {
							$request = new Rabbitmq($this->config['rabbit']['user'], $this->config['rabbit']['password'], $this->config['rabbit']['host']);
							$result = $request->getQueueInfo('/', 'clickhouse');
							$this->logger->info('Queue: ' . $result['messages']);

							//Проверяем, чтобы в очереди было мало записей, чтобы не дублировать клики, когда у нас проблемы.
							if( $result['messages'] < 20000 ) {
								$actions = $this->cli->get("SELECT 1 FROM recone_actions WHERE did = {$this->cli->quote($body['values']['did'])} 
																							AND shop_id = {$body['values']['shop_id']}
																							AND event = 'click'
																							AND item_id = '{$body['values']['object_id']}'
																							AND object_type = 'VendorCampaign'
																							AND object_id = '{$campaign->object_id}'
																							AND recommended_by = '{$body['values']['recommended_by']}'
																							AND date >= '{$date}'
																							AND created_at >= '{$dateTime}'
																							ORDER BY created_at DESC");

								//Если вернулся пустой результат, добавляем событие клика
								$access = empty($actions);
							} else {
								$access = false;
							}
						} catch (\Exception $e) {
							$this->logger->error($e->getMessage() . ' ' . $e->getTraceAsString());
							$access = false;
						}
					}

					//Добавляем событие в очередь
					if( $access ) {
						$this->channel->basic_publish(new AMQPMessage(json_encode([
							'table'  => 'recone_actions',
							'values' => [
								'did'            => $body['values']['did'],
								'sid'            => $body['values']['current_session_code'] ?? '',
								'shop_id'        => $body['values']['shop_id'],
								'event'          => 'click',
								'item_id'        => $body['values']['object_id'],
								'object_type'    => 'VendorCampaign',
								'object_id'      => $campaign->object_id ?? -1,
								'object_price'   => $campaign->object_price ?? -1,
								'price'          => $body['values']['price'],
								'amount'         => 1,
								'brand'          => $body['values']['brand'],
								'recommended_by' => $body['values']['recommended_by'],
								'referer'        => null,
								'position'       => $body['values']['position'] ?? null,
							],
							'opts'   => [],
						])), '', 'clickhouse');
					}
				}
			}
		}
	}
}