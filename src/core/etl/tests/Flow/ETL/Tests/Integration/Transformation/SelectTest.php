<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\select;

final class SelectTest extends FlowTestCase
{
    public function test_select_columns_with_references(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'age' => 25, 'city' => 'New York'],
                ['id' => 2, 'name' => 'Bob', 'age' => 30, 'city' => 'Los Angeles'],
            ]))
            ->transform(select(ref('id'), ref('city')))
            ->fetch()
            ->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'city' => 'New York'],
                ['id' => 2, 'city' => 'Los Angeles'],
            ],
            $rows,
        );
    }

    public function test_select_columns_with_string_names(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'age' => 25, 'city' => 'New York'],
                ['id' => 2, 'name' => 'Bob', 'age' => 30, 'city' => 'Los Angeles'],
            ]))
            ->with(select('id', 'name'))
            ->fetch()
            ->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ],
            $rows,
        );
    }
}
