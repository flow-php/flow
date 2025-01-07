<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{df,
    from_array,
    int_schema,
    list_schema,
    schema,
    type_list,
    type_string};
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Tests\FlowIntegrationTestCase;

final class MapTest extends FlowIntegrationTestCase
{
    public function test_using_map_to_replace_nullable_lists() : void
    {
        $rows = df()
            ->read(
                from_array(
                    [
                        ['id' => 1, 'tags' => ['A', 'B']],
                        ['id' => 2, 'tags' => null],
                        ['id' => 3, 'tags' => ['D']],
                    ]
                )->withSchema(
                    schema(
                        int_schema('id'),
                        list_schema('tags', type_list(type_string(), true))
                    )
                )
            )
            ->map(
                fn (Row $row) : Row => $row->map(fn (Entry $e) => $e->value() === null && $e->is('tags') ? $e->withValue([]) : $e)
            )
            ->fetch();

        self::assertEquals(
            [
                ['id' => 1, 'tags' => ['A', 'B']],
                ['id' => 2, 'tags' => []],
                ['id' => 3, 'tags' => ['D']],
            ],
            $rows->toArray()
        );
    }

    public function test_using_map_to_replace_nulls() : void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => null],
                ['id' => 3, 'name' => 'Doe'],
            ]))
            ->map(
                fn (Row $row) : Row => $row->map(fn (Entry $e) => $e->value() === null && $e->is('name') ? $e->withValue('N/A') : $e)
            )
            ->fetch();

        self::assertEquals(
            [
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'N/A'],
                ['id' => 3, 'name' => 'Doe'],
            ],
            $rows->toArray()
        );
    }
}
