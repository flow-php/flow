<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_array;

final class ArrayLoaderTest extends FlowTestCase
{
    public function test_loads_rows_data_into_memory(): void
    {
        $rows1 = rows(
            schema(int_schema('number'), str_schema('name')),
            row(['number' => 1, 'name' => 'one']),
            row(['number' => 2, 'name' => 'two']),
        );

        $rows2 = rows(
            schema(int_schema('number'), str_schema('name')),
            row(['number' => 3, 'name' => 'three']),
            row(['number' => 4, 'name' => 'four']),
        );

        $array = [];

        $loader = to_array($array);
        $loader->load($rows1, flow_context());
        $loader->load($rows2, flow_context());

        static::assertEquals($rows1->merge($rows2)->toArray(), $array);
    }
}
