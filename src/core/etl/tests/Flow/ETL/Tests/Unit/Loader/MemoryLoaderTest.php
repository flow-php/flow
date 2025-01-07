<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use function Flow\ETL\DSL\{config, flow_context, row, rows};
use function Flow\ETL\DSL\{int_entry, str_entry, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\{Tests\FlowTestCase};

final class MemoryLoaderTest extends FlowTestCase
{
    public function test_loads_rows_data_into_memory() : void
    {
        $rows = rows(row(int_entry('number', 1), str_entry('name', 'one')), row(int_entry('number', 2), str_entry('name', 'two')));

        $memory = new ArrayMemory();

        to_memory($memory)->load($rows, flow_context(config()));

        self::assertEquals($rows->toArray(), $memory->dump());
    }
}
