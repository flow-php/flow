<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Row;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class MapTest extends FlowIntegrationTestCase
{
    public function test_using_map_to_replace_nullable_lists(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'tags' => ['A', 'B']],
                ['id' => 2, 'tags' => null],
                ['id' => 3, 'tags' => ['D']],
            ])->withSchema(schema(int_schema('id'), list_schema('tags', type_list(type_string()), true))))
            ->map(
                schema(int_schema('id'), list_schema('tags', type_list(type_string()))),
                static fn(Row $row): Row => row(['id' => $row->get('id'), 'tags' => $row->get('tags') ?? []]),
            )
            ->fetch();

        static::assertEquals(
            [
                ['id' => 1, 'tags' => ['A', 'B']],
                ['id' => 2, 'tags' => []],
                ['id' => 3, 'tags' => ['D']],
            ],
            $rows->toArray(),
        );
    }

    public function test_using_map_to_replace_nulls(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => null],
                ['id' => 3, 'name' => 'Doe'],
            ]))
            ->map(schema(int_schema('id'), str_schema('name')), static fn(Row $row): Row => row([
                'id' => $row->get('id'),
                'name' => $row->get('name') ?? 'N/A',
            ]))
            ->fetch();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'N/A'],
                ['id' => 3, 'name' => 'Doe'],
            ],
            $rows->toArray(),
        );
    }
}
