<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Async\Socket\Worker;

use Flow\ETL\Async\Socket\Worker\Pool;
use PHPUnit\Framework\TestCase;

final class PoolTest extends TestCase
{
    public function test_checking_if_id_exists_in_pool() : void
    {
        $pool = Pool::generate(5);
        $this->assertFalse($pool->has('not-existing-id'));
        $this->assertTrue($pool->has(\current($pool->ids())));
    }

    public function test_connecting_worker() : void
    {
        $pool = Pool::generate(5);
        $pool->connect($pool->ids()[0]);
        $this->assertCount(1, $pool->onlyConnected());
        $pool->disconnect($pool->ids()[0]);
        $this->assertCount(0, $pool->onlyConnected());
    }
}
