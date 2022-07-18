<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\To;
use Flow\ETL\FlowContext;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class MemoryLoaderTest extends TestCase
{
    public function test_loads_rows_data_into_memory() : void
    {
        $rows = new Rows(
            Row::create(Entry::integer('number', 1), Entry::string('name', 'one')),
            Row::create(Entry::integer('number', 2), Entry::string('name', 'two')),
        );

        $memory = new ArrayMemory();

        To::memory($memory)->load($rows, new FlowContext(Config::default()));

        $this->assertEquals($rows->toArray(), $memory->dump());
    }
}
