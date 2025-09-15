<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Transformation;

use function Flow\ETL\DSL\{df, drop, from_array, ref};
use Flow\ETL\Tests\FlowTestCase;

final class DropTest extends FlowTestCase
{
    public function test_drop_columns_with_references() : void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'age' => 25, 'city' => 'New York'],
                ['id' => 2, 'name' => 'Bob', 'age' => 30, 'city' => 'Los Angeles'],
            ]))
            ->transform(drop(ref('name'), ref('age')))
            ->fetch()
            ->toArray();

        self::assertEquals(
            [
                ['id' => 1, 'city' => 'New York'],
                ['id' => 2, 'city' => 'Los Angeles'],
            ],
            $rows,
        );
    }

    public function test_drop_columns_with_string_names() : void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'age' => 25, 'city' => 'New York'],
                ['id' => 2, 'name' => 'Bob', 'age' => 30, 'city' => 'Los Angeles'],
            ]))
            ->with(drop('age', 'city'))
            ->fetch()
            ->toArray();

        self::assertEquals(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ],
            $rows,
        );
    }
}
