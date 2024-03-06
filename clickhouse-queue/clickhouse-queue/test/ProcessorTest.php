<?php

namespace REES46\ClickHouse\Test;

use League\CLImate\CLImate;
use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Message;
use PHPUnit\Framework\MockObject\MockObject;
use REES46\ClickHouse\Processor;
use REES46\Core\Clickhouse;
use REES46\Test\BaseTest;
use function Amp\async;
use function Amp\Future\awaitAll;

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
		$worker->expects($this->exactly(2))->method('availableForProcessing')->willReturn(true);

		$channel = $this->channel();
		$channel->expects($this->exactly(3))->method('ack');
		awaitAll([
			async(fn() => $worker->received($this->message('events', ['shop_id' => 1, 'client_id' => 1, 'event' => 'view', 'code' => uniqid(), 'category' => 'Item']), $channel)),
			async(fn() => $worker->received($this->message('events', ['shop_id' => 1, 'client_id' => 1, 'event' => 'view', 'code' => uniqid(), 'category' => 'Item']), $channel)),
		]);
		$worker->queueUpdated();

		$this->assertEquals(2, Clickhouse::get()->count('events'));

		$worker->received($this->message('events', ['shop_id' => 1, 'client_id' => 1, 'event' => 'view', 'code' => uniqid(), 'category' => 'Item']), $channel);

		$worker->queueUpdated();

		$this->assertEquals(3, Clickhouse::get()->count('events'));
	}
}
