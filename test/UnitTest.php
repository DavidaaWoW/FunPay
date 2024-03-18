<?php

use PHPUnit\Framework\TestCase;

/**
 *
 */
class UnitTest extends TestCase {
	/**
	 * @return void
	 */
	public function testOk() {
		self::assertEquals(true, 0==false);
	}

	/**
	 * @return void
	 */
	public function testNotEquals() {
		self::assertNotEquals(true, 1==2);
	}
}
