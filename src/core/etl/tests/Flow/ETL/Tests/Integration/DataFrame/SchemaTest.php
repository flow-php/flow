<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Pipeline;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_entry;
use function range;

final class SchemaTest extends FlowIntegrationTestCase
{
    public function test_extraction_according_to_schema(): void
    {
        $rows = df()
            ->read(from_array(
                [
                    ['id' => 1, 'name' => 'name_1', 'active' => null],
                    ['id' => 2, 'name' => 'name_2', 'active' => null],
                    ['id' => 3, 'name' => 'name_3', 'active' => null],
                ],
                $schema = schema(int_schema('id'), str_schema('name'), bool_schema('active', nullable: true)),
            ))
            ->collect()
            ->fetch();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'name_1', 'active' => null],
                ['id' => 2, 'name' => 'name_2', 'active' => null],
                ['id' => 3, 'name' => 'name_3', 'active' => null],
            ],
            $rows->toArray(),
        );
        static::assertEquals($schema, $rows->schema());
    }

    public function test_extraction_without_to_schema(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'name' => 'name_1', 'active' => null],
                ['id' => 2, 'name' => 'name_2', 'active' => null],
                ['id' => 3, 'name' => 'name_3', 'active' => null],
            ]))
            ->collect()
            ->fetch();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'name_1', 'active' => null],
                ['id' => 2, 'name' => 'name_2', 'active' => null],
                ['id' => 3, 'name' => 'name_3', 'active' => null],
            ],
            $rows->toArray(),
        );
        static::assertEquals(
            schema(
                int_schema('id'),
                str_schema('name'),
                str_schema('active', nullable: true, metadata: Metadata::fromArray([Metadata::FROM_NULL => true])),
            ),
            $rows->schema(),
        );
    }

    public function test_getting_schema(): void
    {
        $rows = array_to_rows(
            array_map(
                static fn($i) => [
                    'id' => $i,
                    'name' => 'name_' . $i,
                    'active' => ($i % 2) === 0,
                ],
                range(1, 100),
            ),
            flow_context(config())->entryFactory(),
        );

        static::assertEquals(
            schema(int_schema('id'), str_schema('name'), bool_schema('active')),
            df()->read(from_rows($rows))->autoCast()->schema(),
        );
    }

    public function test_getting_schema_from_limited_rows(): void
    {
        $rows = array_to_rows(
            array_map(
                static fn($i) => [
                    'id' => $i,
                    'name' => 'name_' . $i,
                    'active' => ($i % 2) === 0,
                    'union' => $i > 50 ? 'string' : 1,
                ],
                range(1, 100),
            ),
            flow_context(config())->entryFactory(),
        );

        static::assertEquals(
            schema(int_schema('id'), str_schema('name'), bool_schema('active'), int_schema('union')),
            df()->read(from_rows($rows))->autoCast()->limit(50)->schema(),
        );
    }

    public function test_schema_when_starting_rows_are_null(): void
    {
        $rows = df()
            ->read(from_array([
                ['string' => null, 'bool' => null, 'int' => null, 'float' => null],
                ['string' => 'a', 'bool' => true, 'int' => 1, 'float' => 1.24],
            ]))
            ->collect()
            ->fetch();

        static::assertEquals(
            schema(
                str_schema('string', true),
                bool_schema('bool', true),
                int_schema('int', true),
                float_schema('float', true),
            ),
            $rows->schema(),
        );
    }

    public function test_taking_schema_from_pipeline(): void
    {
        $pipeline = new Pipeline(
            $extractor = from_array([
                ['string' => null, 'bool' => null, 'int' => null, 'float' => null],
                ['string' => 'a', 'bool' => true, 'int' => 1, 'float' => 1.24],
            ]),
        );

        static::assertEquals(
            schema(
                str_schema('string', true),
                bool_schema('bool', true),
                int_schema('int', true),
                float_schema('float', true),
            ),
            Schema::fromPipeline($pipeline, $context = flow_context()),
        );

        static::assertEquals(
            [
                rows(row(null_entry('string'), null_entry('bool'), null_entry('int'), null_entry('float'))),
                rows(row(
                    string_entry('string', 'a'),
                    bool_entry('bool', true),
                    int_entry('int', 1),
                    float_entry('float', 1.24),
                )),
            ],
            iterator_to_array($extractor->extract($context)),
        );
    }
}
