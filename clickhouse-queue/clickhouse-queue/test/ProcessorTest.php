<?php

namespace REES46\ClickHouse\Test;

use League\CLImate\CLImate;
use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Message;
use PHPUnit\Framework\MockObject\MockObject;
use REES46\ClickHouse\Processor;
use REES46\Core\Clickhouse;
use REES46\Test\BaseTest;

class ProcessorTest extends BaseTest {

	/**
	 * @param string $table
	 * @param array  $data
	 * @return Message
	 */
	protected function message(string $table, array $data) {
		return new Message(json_encode($data), '', '', headers: ['table' => $table]);
	}

	/**
	 * @return Channel|MockObject
	 */
	protected function channel(): Channel {
		return $this->getMockBuilder(Channel::class)->disableOriginalConstructor()->getMock();
	}

	public function testInsert() {
		$this->initClickhouseTable('events');
		$this->assertEquals(0, Clickhouse::get()->count('events'));

		$worker = $this->getMockBuilder(Processor::class)->onlyMethods(['availableForProcessing'])->setConstructorArgs([new CLImate()])->getMock();
		$worker->expects($this->once())->method('availableForProcessing')->willReturn(true);

		$message = $this->message('events', ['shop_id' => 1, 'client_id' => 1, 'event' => 'view', 'code' => uniqid(), 'category' => 'Item']);
		$channel = $this->channel();
		$channel->expects($this->once())->method('ack')->with($message);
		$worker->received($message, $channel);
		$worker->queueUpdated();

		$this->assertEquals(1, Clickhouse::get()->count('events'));
	}
}
