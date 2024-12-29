<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{df, from_array, ref};
use Flow\ETL\Tests\Integration\FlowIntegrationTestCase;

final class FilterTest extends FlowIntegrationTestCase
{
    public function test_multiple_filters() : void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'John', 'age' => 20],
                ['id' => 2, 'name' => 'Jane', 'age' => 25],
                ['id' => 3, 'name' => 'Doe', 'age' => 30],
                ['id' => 4, 'name' => 'Smith', 'age' => 35],
                ['id' => 5, 'name' => 'Brown', 'age' => 40],
            ]))
            ->filters([
                ref('age')->greaterThanEqual(30),
                ref('name')->startsWith('S'),
            ])
            ->fetch();

        self::assertSame(
            [
                ['id' => 4, 'name' => 'Smith', 'age' => 35],
            ],
            $rows->toArray()
        );
    }

    public function test_single_filter() : void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'John', 'age' => 20],
                ['id' => 2, 'name' => 'Jane', 'age' => 25],
                ['id' => 3, 'name' => 'Doe', 'age' => 30],
                ['id' => 4, 'name' => 'Smith', 'age' => 35],
                ['id' => 5, 'name' => 'Brown', 'age' => 40],
            ]))
            ->filter(ref('age')->greaterThanEqual(30))
            ->fetch();

        self::assertSame(
            [
                ['id' => 3, 'name' => 'Doe', 'age' => 30],
                ['id' => 4, 'name' => 'Smith', 'age' => 35],
                ['id' => 5, 'name' => 'Brown', 'age' => 40],
            ],
            $rows->toArray()
        );
    }
}
