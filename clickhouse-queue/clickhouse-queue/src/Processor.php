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
use REES46\Bulk\Worker\BulkWorker;
use REES46\Core\Clickhouse;
use REES46\Core\RabbitMQ;
use REES46\Worker\AbstractWorker;
use REES46\Worker\WorkerTrait;
use REES46\Core\Logger;
use Workerman\Worker;

/**
 * ClickHouse Daemon
 * @package REES46\ClickHouse
 */
class Processor extends AbstractWorker {

	private GeoDetector $geo_detector;

	private array $queue_time = [];
	private bool $started = false;
	private string $loop_check;
	private string $loop_queue;

	public function onReload() {
		Loop::cancel($this->loop_check);
		Loop::cancel($this->loop_queue);
		//Отписываемся от основного канала
		RabbitMQ::get()->channel->cancel('clickhouse-' . getmypid());
		Logger::$logger->warning('Reload');
		Loop::delay(3000, function() {
			Logger::$logger->warning('Terminate on reload');
			exit;
		});
	}

	/**
	 * @return \Generator
	 */
	public function onStart() {
		//Подключаемся
		yield RabbitMQ::get()->connect();
		Clickhouse::$timeout = 30000;
		Clickhouse::$inactive_timeout = 30000;
		Clickhouse::$transfer_timeout = 30000;

		//Устанавливаем время в UTC, т.к. база вся работает только с ним
		date_default_timezone_set('UTC');
		Logger::$logger->info('Starting');

		//Инициализируем определение местоположения
		$this->geo_detector = new GeoDetector($this->config['geo']);

		//Сначала обрабатываем очередь
		yield $this->queueUpdated();
		yield $this->queueUpdated(true);
		Logger::$logger->info('Queue cleared');
		$this->started = true;
		sleep(1);

		//Указываем лимит неподтвержденных тасков
		yield RabbitMQ::get()->channel->qos(0, 500);

		//Подписываемся на канал
		yield RabbitMQ::get()->channel->queueDeclare(RabbitMQ::CLICKHOUSE_QUEUE, false, true);

		//Подписываем колбек функцию
		yield RabbitMQ::get()->channel->consume([$this, 'received'], RabbitMQ::CLICKHOUSE_QUEUE, 'clickhouse-' . getmypid());

		//Каждую минуту проверяем коннект
		$this->loop_check = Loop::repeat(60000, fn() => $this->checkConnection());
		//Запускаем обработку очередей каждые 20 секунд
		$this->loop_queue = Loop::repeat(20000, fn() => $this->queueUpdated());
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

			//Форматируем вставляемые данные
			$values = yield Clickhouse::get()->format($body['table'], $body['values']);

			//Добавляем дефолтные данные
			$values = Clickhouse::get()->setDefaultValues($body['table'], $values);

			//Добавляем строку в файл вставки
			file_put_contents($this->dumpPath($body['table']), '(' . implode(',', $values) . ')' . PHP_EOL, FILE_APPEND);

			//Запоминаем время вставки
			if( !isset($this->queue_time[$body['table']]) ) {
				$this->queue_time[$body['table']] = time();
			}

			//Подтверждаем прием
			yield $channel->ack($message);

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
			Logger::$logger->error(get_class($e) . ', ' . $e->getMessage() . ', message: ' . $message->content, array_slice($e->getTrace(), 0, 2));
			\Amp\Loop::delay(1000, fn() => RabbitMQ::get()->channel->reject($message));
		}
	}

	/**
	 * Возвращает путь к файлу для вставки
	 * @param string $table
	 * @return string
	 */
	protected function dumpPath(string $table) {
		return APP_ROOT . '/tmp/' . $table . '.sql';
	}

	protected function checkConnection() {
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
		});
	}

	/**
	 * Обрабатывает внутреннюю очередь чтобы вставить данные пачкой
	 * @param bool $force Форсированная обработка упавших файлов
	 * @return Promise
	 */
	protected function queueUpdated($force = false) {
		return \Amp\call(function() use ($force) {
			//Проходим по локальной очереди
			foreach( glob(pathinfo($this->dumpPath('1'), PATHINFO_DIRNAME) . '/*.sql' . ($force ? '.*' : '')) as $filename ) {
				$table = pathinfo(preg_replace('/\.sql(\..*?)?$/', '.sql', $filename), PATHINFO_FILENAME);
				//Для обработки сдохших файлов
				if( $force && preg_match('/\.sql(\..*?)?$/', $filename) ) {
					rename($filename, $this->dumpPath($table));
				}
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
	 * @param $table
	 * @return Promise
	 */
	protected function queueProcessing($table) {
		return \Amp\call(function() use ($table) {
			//Копируем данные, чтобы во время обработки их не дополнили случайно
			$path = $this->dumpPath($table) . '.' . uniqid();
			rename($this->dumpPath($table), $path);

			unset($this->queue_time[$table]);

			try {
				$data = file_get_contents($path);
				yield Clickhouse::get()->bulkDataInsert($data, $table);
				unlink($path);
			} catch (\Exception $e) {
				Logger::$logger->error(get_class($e) . ', ' . $e->getMessage(), array_slice($e->getTrace(), 0, 2));
			}

			//Дополнительные алгоритмы при вставке в таблицу
			//foreach( $bulk as $data ) {
			//	$time = microtime(true);
			//	try {
			//		yield $this->tableProcessor($data['body']);
			//	} catch (\Exception $e) {
			//		Logger::$logger->error($e->getMessage(), $e->getTrace());
			//	} finally {
			//		if( Logger::$logger->isHandling(\Monolog\Logger::INFO) ) {
			//			Logger::$logger->info("\e[1;36" . "mProcessing table $table \e[1;35m(" . round((microtime(true) - $time) * 1000, 2) . "ms)\e[0m");
			//		}
			//	}
			//}
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
								Logger::$logger->error(get_class($e) . ', ' . $e->getMessage() . ' ' . $e->getTraceAsString());
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