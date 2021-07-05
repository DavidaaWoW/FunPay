<?php
namespace REES46\ClickHouse;

use Amp\Loop;
use Amp\Promise;
use Amp\Socket\ConnectException;
use League\CLImate\CLImate;
use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Exception\ConnectionException;
use PHPinnacle\Ridge\Message;
use PHPinnacle\Ridge\Queue;
use REES46\Core\Clickhouse;
use REES46\Core\RabbitMQ;
use REES46\Worker\WorkerTrait;
use REES46\Core\Logger;
use Workerman\Worker;

/**
 * ClickHouse Daemon
 * @package REES46\ClickHouse
 */
class Processor extends Worker {
	use WorkerTrait;

	private GeoDetector $geo_detector;

	private int $batch_size;
	private array $batch = [];
	private array $queue = [];
	private array $queue_time = [];

	/**
	 * API WebServer constructor.
	 * @param CLImate $cli
	 * @param array   $config
	 */
	public function __construct(CLImate $cli, array $config) {
		parent::__construct();
		$this->configure($cli, $config, 'Queue worker', 'rees46-clickhouse-queue');
	}

	/**
	 * @return \Generator
	 */
	public function onStart() {
		//Подключаемся
		yield RabbitMQ::get()->connect();

		//Устанавливаем время в UTC, т.к. база вся работает только с ним
		date_default_timezone_set('UTC');
		Logger::$logger->info('Starting');

		//Проверяем, чтобы максимальное число в пачке для таблиц было меньше или равно общей
		$this->batch_size = $this->config['default']['batch_size'] ?? 1000;
		$this->batch = $this->config['default']['batch'] ?? [];

		//Инициализируем определение местоположения
		$this->geo_detector = new GeoDetector($this->config['geo']);

		//Указываем лимит неподтвержденных тасков
		yield RabbitMQ::get()->channel->qos(0, 15000);

		//Подписываемся на канал
		yield RabbitMQ::get()->channel->queueDeclare(RabbitMQ::CLICKHOUSE_QUEUE, false, true);

		//Подписываем колбек функцию
		yield RabbitMQ::get()->channel->consume([$this, 'received'], RabbitMQ::CLICKHOUSE_QUEUE, getmypid());

		//Запускаем обработку очередей каждые 10 секунд
		Loop::repeat(10000, fn() => $this->queueUpdated());
		Logger::$logger->info('Started');
	}

	/**
	 * Получили сообщение
	 * @param Message $message
	 * @param Channel $channel
	 * @return \Generator
	 * @throws ConnectException
	 */
	public function received(Message $message, Channel $channel) {
		try {
			//Делаем работу
			$body = $this->beforeTableProcessor(json_decode($message->content, true));

			//Проверяем очередь
			if( !isset($this->queue[$body['table']]) ) {
				$this->queue[$body['table']] = [];
				$this->queue_time[$body['table']] = time();
			}

			//Добавляем в очередь
			$this->queue[$body['table']][$message->deliveryTag] = [
				'body'    => $body,
				'message' => $message,
			];

			//Если набрали данных на пачку
			if( $this->availableForProcessing($body['table']) ) {
				yield $this->queueProcessing($body['table']);
			}
		} catch (ConnectionException | ConnectException $e) {
			sleep(10);
			//При потере коннекта к раббиту, падаем
			//Воркер перезапустит процесс и создаст все коннекты снова
			throw $e;
		} catch (\Throwable $e) {
			Logger::$logger->error($e->getMessage(), array_slice($e->getTrace(), 0, 2));
			\Amp\Loop::delay(1000, fn() => RabbitMQ::get()->channel->reject($message));
			if( !empty($body) ) {
				unset($this->queue[$body['table']][$message->deliveryTag]);
			}
		}
	}

	/**
	 * Обрабатывает внутреннюю очередь чтобы вставить данные пачкой
	 */
	protected function queueUpdated() {
		return \Amp\call(function() {
			//Проверяем коннект
			try {
				if( !RabbitMQ::get()->isConnected() ) {
					throw new ConnectionException('Not connected');
				}
				/** @var Channel $channel */
				$channel = yield RabbitMQ::get()->client->channel();
				$channel->qos(0, 1);
				/** @var Queue $queue */
				$queue = yield $channel->queueDeclare(RabbitMQ::CLICKHOUSE_QUEUE, true);
				Logger::$logger->debug('Messages: ' . $queue->messages());
				$channel->close();
			} catch (ConnectionException $e) {
				sleep(10);
				throw $e;
			}

			//Проходим по локальной очереди
			foreach( $this->queue as $table => $data ) {
				if( $this->availableForProcessing($table) ) {
					yield $this->queueProcessing($table);
				}
			}
		});
	}

	/**
	 * Проверяет, готова ли очередь для вставки в таблицу
	 * @param $table
	 * @return bool
	 */
	protected function availableForProcessing($table) {
		if( empty($this->queue[$table]) ) {
			return false;
		}

		if( isset($this->queue_time[$table]) && $this->queue_time[$table] <= strtotime('-10 seconds') ) {
			return true;
		}

		//Получаем количество данных в очереди для вставки
		$count = count($this->queue[$table]);

		//Если текущая таблица присутствует в списке
		if( isset($this->batch[$table]) ) {
			return $count >= $this->batch[$table];
		}

		//Общие условия для вставки
		return $count >= $this->batch_size;
	}

	/**
	 * Запускает обработку очереди для указанной таблицы
	 * @param $table
	 * @return Promise
	 */
	protected function queueProcessing($table) {
		return \Amp\call(function() use ($table) {
			$time = microtime(true);
			//Копируем данные, чтобы во время обработки их не дополнили случайно
			$bulk = (new \ArrayObject($this->queue[$table]))->getArrayCopy();
			unset($this->queue[$table]);
			unset($this->queue_time[$table]);

			try {
				yield Clickhouse::get()->bulkInsert($table, array_map(fn($v) => $v['body']['values'], $bulk));

				//Отвечаем, что успешно
				foreach( $bulk as $data ) {
					yield RabbitMQ::get()->channel->ack($data['message']);
				}

				if( Logger::$logger->isHandling(\Monolog\Logger::INFO) ) {
					Logger::$logger->info("\e[1;36" . "mProcessing \e[1;35m(" . round((microtime(true) - $time) * 1000, 2) . "ms)\e[0m \e[1;34m" . 'update ' . $table . ': ' . count($bulk) . "\e[0m");
				}

				//todo отключили обработку для реквана
				$bulk = [];
			} catch (\Exception $e) {
				Logger::$logger->error($e->getMessage(), array_slice($e->getTrace(), 0, 2));
				\Amp\Loop::delay(10000, function() use ($bulk) {
					foreach( $bulk as $data ) {
						yield RabbitMQ::get()->channel->reject($data['message'], true);
					}
				});
			}

			//Дополнительные алгоритмы при вставке в таблицу
			foreach( $bulk as $data ) {
				try {
					yield $this->tableProcessor($data['body']);
				} catch (\Exception $e) {
					Logger::$logger->error($e->getMessage(), $e->getTrace());
				}
			}
		});
	}

	/**
	 * Дополняет данные при вставке в таблицу
	 * @param array $body
	 * @return array
	 */
	protected function beforeTableProcessor(array $body) {
		switch( $body['table'] ) {

			//Дополняем данными о местоположении по ip
			//deprecated
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
	 * @return Promise
	 */
	protected function tableProcessor(array $body) {
		return \Amp\call(function() use ($body) {
			switch( $body['table'] ) {

				//Таблица событий
				case 'events':
					yield $this->eventsProcessor($body);
					break;

				//Таблица заказов
				case 'order_items':
					yield $this->orderItemProcessor($body);
					break;
			}
		});
	}

	/**
	 * Обработчик таблицы заказа
	 * @param array $body
	 * @return Promise
	 */
	protected function orderItemProcessor(array $body) {
		return \Amp\call(function() use ($body) {

			//Если дополнительные параметры или бренд не указаны, выходим
			if( empty($body['opts']) || empty($body['values']['brand']) || empty($body['values']['did']) ) {
				return;
			}

			//Дата выборки
			$date = date('Y-m-d', strtotime('-2 DAYS'));

			//Получаем список компаний
			$campaigns = Clickhouse::get()->execute("SELECT DISTINCT object_id, brand FROM recone_actions WHERE did = :did AND shop_id = :shop_id AND event = 'click' AND item_id = :item_id AND object_type = 'VendorCampaign' AND date >= '{$date}'", [
				'did'     => $body['values']['did'],
				'shop_id' => $body['values']['shop_id'],
				'item_id' => $body['values']['item_uniqid'],
			]);

			//Если нашлись компании вендоров
			if( !empty($campaigns) ) {
				foreach( $campaigns as $campaign ) {

					//Добавляем событие в очередь
					yield RabbitMQ::get()->push('recone_actions', [
						'did'            => $body['values']['did'],
						'sid'            => $body['opts']['current_session_code'] ?? '',
						'shop_id'        => $body['values']['shop_id'],
						'event'          => 'purchase',
						'item_id'        => $body['values']['item_uniqid'],
						'object_type'    => 'VendorCampaign',
						'object_id'      => $campaign['object_id'],
						'object_price'   => 0,
						'price'          => $body['values']['price'],
						'amount'         => $body['values']['amount'] ?? 1,
						'brand'          => $body['values']['brand'],
						'recommended_by' => $body['values']['recommended_by'] ?? null,
						'referer'        => null,
						'position'       => $body['values']['position'] ?? null,
					]);
				}
			}
		});
	}

	/**
	 * Обработчик таблицы событий
	 * @param $body
	 * @return Promise
	 */
	protected function eventsProcessor($body) {
		return \Amp\call(function() use ($body) {

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
			if( in_array($body['values']['event'], ['view', 'cart', 'purchase']) && $body['values']['category'] == 'Item' && $body['values']['recommended_by'] && $body['values']['brand'] ) {

				//Получаем последнюю. Если будут проблемы с простановкой флага recommended_by при просмотре товара, просто убрать фильтрацию и брать recommended_by из события.
				$campaigns = yield Clickhouse::get()->execute("SELECT DISTINCT object_id, object_price, brand FROM recone_actions
					WHERE did = :did AND shop_id = :shop_id AND event = :event AND item_id = :item_id AND object_type = :object_type AND recommended_by = :recommended_by
								AND date >= '{$date}' AND created_at >= '{$dateTime}' ORDER BY created_at DESC", [
					'did'            => $body['values']['did'],
					'shop_id'        => $body['values']['shop_id'],
					'event'          => 'view',
					'item_id'        => $body['values']['label'],
					'object_type'    => 'VendorCampaign',
					'recommended_by' => $body['values']['recommended_by'],
				]);

				//Если нашлись компании вендоров
				if( !empty($campaigns) ) {
					foreach( $campaigns as $campaign ) {

						//Если событие просмотра товара, то разрешаем добавлять клик
						$access = $body['values']['event'] == 'view';

						//Проверяем, чтобы не было повторного клика в течении часа только для текущей кампании
						if( $body['values']['event'] == 'view' ) {
							$actions = yield Clickhouse::get()->execute("SELECT 1 FROM recone_actions WHERE did = :did AND shop_id = :shop_id AND event = :event AND object_type = :object_type AND object_id = :object_id
							  AND date >= '{$dateHour}' AND created_at >= '{$dateTimeHour}' ORDER BY created_at DESC", [
								'did'         => $body['values']['did'],
								'shop_id'     => $body['values']['shop_id'],
								'event'       => 'click',
								'object_type' => 'VendorCampaign',
								'object_id'   => $campaign['object_id'],
							]);
							//Если вернулся пустой результат, добавляем событие клика
							$access = empty($actions);
						}

						//Если событие корзины или покупки и клика не было
						if( in_array($body['values']['event'], ['cart', 'purchase']) && $access ) {

							try {
								$actions = yield Clickhouse::get()->execute("SELECT 1 FROM recone_actions WHERE did = :did AND shop_id = :shop_id AND event = :event AND item_id = :item_id AND object_type = :object_type AND object_id = :object_id AND recommended_by = :recommended_by
								  AND date >= '{$date}' AND created_at >= '{$dateTime}' ORDER BY created_at DESC", [
									'did'            => $body['values']['did'],
									'shop_id'        => $body['values']['shop_id'],
									'event'          => 'click',
									'item_id'        => $body['values']['label'],
									'object_type'    => 'VendorCampaign',
									'object_id'      => $campaign['object_id'],
									'recommended_by' => $body['values']['recommended_by'],
								]);

								//Если вернулся пустой результат, добавляем событие клика
								$access = empty($actions);
							} catch (\Exception $e) {
								Logger::$logger->error($e->getMessage() . ' ' . $e->getTraceAsString());
								$access = false;
							}
						}

						//Добавляем событие в очередь
						if( $access ) {
							yield RabbitMQ::get()->push('recone_actions', [
								'did'            => $body['values']['did'],
								'sid'            => $body['values']['sid'] ?? '',
								'shop_id'        => $body['values']['shop_id'],
								'event'          => 'click',
								'item_id'        => $body['values']['label'],
								'object_type'    => 'VendorCampaign',
								'object_id'      => $campaign['object_id'] ?? -1,
								'object_price'   => $campaign['object_price'] ?? -1,
								'price'          => $body['values']['price'],
								'amount'         => 1,
								'brand'          => $body['values']['brand'],
								'recommended_by' => $body['values']['recommended_by'],
								'referer'        => null,
								'position'       => $body['values']['position'] ?? null,
							]);
						}
					}
				}
			}
		});
	}
}