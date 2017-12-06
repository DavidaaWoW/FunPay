<?php

namespace REES46\ClickHouse;

/**
 * Клиент к кликхаусу
 * @package REES46\ClickHouse
 */
class Cli {

	/**
	 * @var Logger
	 */
	private $logger;

	private $ch;

	/**
	 * Схема таблиц
	 * @var array
	 */
	private $schema = [];

	/**
	 * Cli constructor.
	 * @param array $config
	 */
	public function __construct($config, $logger) {
		$this->logger = $logger;
		$this->ch = curl_init();
		curl_setopt($this->ch, CURLOPT_URL, "http://{$config['host']}:{$config['port']}/?database={$config['database']}");
		curl_setopt($this->ch, CURLOPT_POST, 1);
		curl_setopt($this->ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($this->ch, CURLOPT_FOLLOWLOCATION, true);
	}

	/**
	 * @param string $table
	 * @param array $values
	 */
	public function insert($table, array $values) {

		//Форматируем вставляемые данные
		$values = $this->format($table, $values);

		//Строим строку вставки
		$sql = sprintf('INSERT INTO %s (%s) VALUES (%s)', $table, implode(',', array_keys($values)), implode(',', $values));
		$this->post($sql);
	}

	/**
	 * Вставляет данные пачкой
	 * @param $table
	 * @param array $data
	 * @throws ProcessorException
	 */
	public function bulkInsert($table, array $data) {

		//Получаем список полей таблицы
		$columns = array_keys($this->schema($table));
		$insert_values = [];
		foreach( $data as $body ) {

			$value = [];
			foreach( $columns as $column ) {
				//Если есть данные колонки
				if( isset($body['values'][$column]) ) {
					$value[$column] = $body['values'][$column];
				} else {
					switch($column) {
						case 'created_at':
							$value[$column] = date('Y-m-d H:i:s');
							break;
						case 'date':
							$value[$column] = date('Y-m-d');
							break;
						default:
							$value[$column] = 'NULL';
					}
				}
			}

			//Форматируем вставляемые данные
			$values = $this->format($table, $value);

			//Добавляем в общий массив
			$insert_values[] = implode(',', $values);
		}

		//Строим строку вставки
		$sql = sprintf('INSERT INTO %s (%s) VALUES (%s)', $table, implode(',', $columns), implode('), (', $insert_values));
		$this->post($sql);
	}

	/**
	 * Отправляет данные
	 * @param string $sql
	 * @return string
	 * @throws ProcessorException
	 */
	public function post($sql) {
		return $this->sql($sql);
	}

	/**
	 * Получение данных
	 * @param $sql
	 * @return null|\stdClass
	 */
	public function get($sql) {
		$result = json_decode($this->sql(preg_replace('/(\n|\t+)/', ' ', $sql) . ' FORMAT JSON'));
		if( isset($result->data) ) {
			return $result->data;
		}
		return null;
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

	/**
	 * @param $sql
	 * @return string
	 * @throws ProcessorException
	 */
	private function sql($sql) {
		$this->logger->debug('SQL: ' . $sql);

		//Отправляем данные
		curl_setopt($this->ch, CURLOPT_POSTFIELDS, $sql);
		$response = curl_exec($this->ch);
		$code = curl_getinfo($this->ch, CURLINFO_HTTP_CODE);

		if( $code >= 400 || $code < 200 ) {
			throw new ProcessorException('[CODE: ' . $code . '] SQL: ' . $sql . PHP_EOL . trim($response));
		}

		return $response;
	}

	/**
	 * Преобразует все данные в формат таблицы
	 * @param string $table
	 * @param array  $values
	 * @return array
	 */
	private function format($table, array $values) {

		//Получаем структуру таблицы
		$this->schema($table);

		foreach($values as $column => $value) {
			$values[$column] = $this->convertToSQLType($table, $column, $value);
		}

		return $values;
	}

	/**
	 * Получает схему таблицы
	 * @param $table
	 * @return array
	 */
	private function schema($table) {
		if( empty($this->schema[$table]) ) {

			//Получаем данные таблицы
			$data = json_decode($this->sql('DESCRIBE TABLE ' . $table . ' FORMAT JSON'), true)['data'];

			//Заполняем данныеми
			$this->schema[$table] = [];
			foreach($data as $column) {
				$this->schema[$table][$column['name']] = [
					'type' => $this->columnConvertType($column['type']),
					'null' => $this->columnIsNull($column['type']),
				];
			}
		}
		return $this->schema[$table];
	}

	/**
	 * Преобразует тип из кликхауса
	 * @param $type
	 * @return null|string
	 */
	private function columnConvertType($type) {
		if( preg_match('/^Nullable\((.*?)\)$/', $type, $match) ) {
			$type = $match[1];
		}
		switch( $type ) {
			case 'Int8':
			case 'Int32':
			case 'Int64':
			case 'UInt8':
			case 'UInt32':
			case 'UInt64':
				return 'int';
			case 'Float32':
			case 'Float64':
				return 'float';
			case 'String':
			case 'Date':
			case 'DateTime':
				return 'string';
		}
		return null;
	}

	/**
	 * Может ли колонка быть NULL
	 * @param $type
	 * @return boolean
	 */
	private function columnIsNull($type) {
		return (bool) preg_match('/^Nullable\((.*?)\)$/', $type, $match);
	}

	protected function convertToSQLType($table, $column, $value) {
		$schema = $this->schema($table)[$column];

		//Если колонка может быть NULL
		if( $schema['null'] && !$value ) {
			return 'NULL';
		}

		settype($value, $schema['type']);
		return $this->quote($value);
	}
}