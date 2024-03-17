<?php

use PHPUnit\Framework\TestCase;

class UnitTest extends TestCase {
	public function testOk() {
		self::assertEquals(true, 0==false);
	}

	public function testNotEquals() {
		self::assertNotEquals(true, 1==2);
	}
}
