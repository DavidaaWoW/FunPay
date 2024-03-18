<?php

namespace FpDbTest\src;

interface DatabaseInterface
{
	/**
	 * @param string $query
	 * @param array  $args
	 * @return string
	 */
	public function buildQuery(string $query, array $args = []): string;

	/**
	 * @return mixed
	 */
	public function skip();
}
