<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_array;

final class ArrayLoaderTest extends FlowTestCase
{
    public function test_loads_rows_data_into_memory(): void
    {
        $rows1 = array_to_rows(
            [['number' => 1, 'name' => 'one'], ['number' => 2, 'name' => 'two']],
            schema(int_schema('number'), str_schema('name')),
        );

        $rows2 = array_to_rows(
            [['number' => 3, 'name' => 'three'], ['number' => 4, 'name' => 'four']],
            schema(int_schema('number'), str_schema('name')),
        );

        $rows3 = array_to_rows([['number' => 5, 'name' => 'five']], schema(int_schema('number'), str_schema('name')));

        $array = [];

        $loader = to_array($array);
        $loader->load($rows1, flow_context());
        $loader->load($rows2, flow_context());
        $loader->load($rows3, flow_context());

        static::assertSame(
            [
                ['number' => 1, 'name' => 'one'],
                ['number' => 2, 'name' => 'two'],
                ['number' => 3, 'name' => 'three'],
                ['number' => 4, 'name' => 'four'],
                ['number' => 5, 'name' => 'five'],
            ],
            $array,
        );
    }
}
