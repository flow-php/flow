<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\coalesce;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class ReplaceNullsTest extends FlowIntegrationTestCase
{
    public function test_replacing_null_lists_with_an_empty_list(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'tags' => ['A', 'B']],
                ['id' => 2, 'tags' => null],
                ['id' => 3, 'tags' => ['D']],
            ])->withSchema(schema(int_schema('id'), list_schema('tags', type_list(type_string()), true))))
            ->withEntry('tags', coalesce(ref('tags'), lit([])))
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

    public function test_replacing_null_strings_with_a_default(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => null],
                ['id' => 3, 'name' => 'Doe'],
            ]))
            ->withEntry('name', coalesce(ref('name'), lit('N/A')))
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
