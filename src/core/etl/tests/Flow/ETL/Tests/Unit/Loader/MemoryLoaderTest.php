<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_memory;

final class MemoryLoaderTest extends FlowTestCase
{
    public function test_loads_rows_data_into_memory(): void
    {
        $rows = array_to_rows(
            [['number' => 1, 'name' => 'one'], ['number' => 2, 'name' => 'two']],
            schema(int_schema('number'), str_schema('name')),
        );

        $memory = new ArrayMemory();

        to_memory($memory)->load($rows, flow_context(config()));

        static::assertEquals($rows->toArray(), $memory->dump());
    }
}
