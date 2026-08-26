<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Pipeline;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_structure;
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
        static::assertEquals(schema(int_schema('id'), str_schema('name'), null_schema('active')), $rows->schema());
    }

    public function test_getting_schema_of_a_column_holding_both_an_empty_array_and_a_structure(): void
    {
        static::assertEquals(
            schema(json_schema('a')),
            df()->read(from_array([['a' => []], ['a' => ['x' => 1]]]))->schema(),
        );
    }

    public function test_getting_schema_of_a_structure_with_a_nested_empty_array(): void
    {
        static::assertEquals(
            // [] detects as list<null>: an empty array is a container with no observed element
            schema(structure_schema('body', type_structure([
                'data' => type_list(type_null()),
                'id' => type_integer(),
            ]))),
            df()->read(from_array([['body' => ['data' => [], 'id' => 1]]]))->schema(),
        );
    }

    public function test_getting_schema_of_a_structure_with_a_nested_heterogeneous_array(): void
    {
        static::assertEquals(
            schema(structure_schema('body', type_structure(['data' => type_json(), 'id' => type_integer()]))),
            df()->read(from_array([['body' => ['data' => [1, 'a'], 'id' => 1]]]))->schema(),
        );
    }

    public function test_getting_schema_of_a_column_holding_unrelated_types(): void
    {
        static::assertEquals(schema(str_schema('a')), df()->read(from_array([['a' => 1], ['a' => true]]))->schema());
    }

    public function test_getting_schema_of_enum_column_with_null_values(): void
    {
        static::assertEquals(
            schema(enum_schema('status', BackedStringEnum::class, nullable: true)),
            df()
                ->read(from_rows(rows(
                    row(enum_entry('status', BackedStringEnum::one)),
                    row(enum_entry('status', null)),
                )))
                ->schema(),
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
            flow_context(config())->hydrator(),
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
            flow_context(config())->hydrator(),
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

        // The extractor derives one schema before it yields, so row 1's nulls are TYPED nulls in
        // known columns rather than untyped NullEntries a later row contradicts.
        $batches = iterator_to_array($extractor->extract($context));

        static::assertSame(
            [
                [['string' => null, 'bool' => null, 'int' => null, 'float' => null]],
                [['string' => 'a', 'bool' => true, 'int' => 1, 'float' => 1.24]],
            ],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $batches),
        );
        static::assertInstanceOf(StringEntry::class, $batches[0]->first()->get('string'));
        static::assertInstanceOf(BooleanEntry::class, $batches[0]->first()->get('bool'));
        static::assertInstanceOf(IntegerEntry::class, $batches[0]->first()->get('int'));
        static::assertInstanceOf(FloatEntry::class, $batches[0]->first()->get('float'));
    }
}
