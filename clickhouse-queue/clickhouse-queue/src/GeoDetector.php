<?php

namespace REES46\ClickHouse;

use GeoIp2\Database\Reader;
use GeoIp2\Exception\AddressNotFoundException;
use MaxMind\Db\Reader\InvalidDatabaseException;

/**
 * Class GeoDetector
 * @package REES46\ClickHouse
 */
class GeoDetector {

	/**
	 * @var Reader
	 */
	private Reader $city_reader;

	/**
	 * @var Reader
	 */
	private Reader $country_reader;

	/**
	 * GeoDetector constructor.
	 * @param array $config
	 * @throws InvalidDatabaseException
	 */
	public function __construct(array $config) {
		// This creates the Reader object, which should be reused across lookups.
		$this->city_reader = new Reader($config['city']);
		$this->country_reader = new Reader($config['country']);
	}

	/**
	 * Определение местоположения
	 * @param string $ip
	 * @return array
	 */
	public function detect(string $ip) {
		try {
			$city = $this->city_reader->city($ip);
			$country = $this->country_reader->country($ip)->country->name;
		} catch (AddressNotFoundException|InvalidDatabaseException $e) {
			$city = null;
			$country = null;
		}

		return [
			'country'    => $country,
			'city'      => $city->city->name ?? null,
			'latitude'  => $city->location->latitude ?? null,
			'longitude' => $city->location->longitude ?? null,
		];
	}
}