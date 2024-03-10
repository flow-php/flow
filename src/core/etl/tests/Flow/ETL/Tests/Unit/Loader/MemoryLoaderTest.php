<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use function Flow\ETL\DSL\{int_entry, str_entry, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\{Config, FlowContext, Row, Rows};
use PHPUnit\Framework\TestCase;

final class MemoryLoaderTest extends TestCase
{
    public function test_loads_rows_data_into_memory() : void
    {
        $rows = new Rows(
            Row::create(int_entry('number', 1), str_entry('name', 'one')),
            Row::create(int_entry('number', 2), str_entry('name', 'two')),
        );

        $memory = new ArrayMemory();

        to_memory($memory)->load($rows, new FlowContext(Config::default()));

        self::assertEquals($rows->toArray(), $memory->dump());
    }
}
