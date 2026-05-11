<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\to_array;

final class ArrayLoaderTest extends FlowTestCase
{
    public function test_loads_rows_data_into_memory(): void
    {
        $rows1 = rows(
            row(int_entry('number', 1), str_entry('name', 'one')),
            row(int_entry('number', 2), str_entry('name', 'two')),
        );

        $rows2 = rows(
            row(int_entry('number', 3), str_entry('name', 'three')),
            row(int_entry('number', 4), str_entry('name', 'four')),
        );

        $array = [];

        $loader = to_array($array);
        $loader->load($rows1, flow_context());
        $loader->load($rows2, flow_context());

        static::assertEquals($rows1->merge($rows2)->toArray(), $array);
    }
}
