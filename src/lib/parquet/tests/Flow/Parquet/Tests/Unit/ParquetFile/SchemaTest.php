<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\union_schema;
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Thrift\SchemaElement;

final class SchemaTest extends TestCase
{
    public function test_calculating_repetition_and_definition_for_nested_fields() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('int'),
            NestedColumn::struct(
                'nested',
                [
                    FlatColumn::int32('int'),
                    FlatColumn::string('strings'),
                    NestedColumn::struct(
                        'nested',
                        [
                            FlatColumn::boolean('bool'),
                        ]
                    ),
                    NestedColumn::list('list_of_ints', ListElement::int32()),
                ]
            ),
        );

        $this->assertSame(1, $schema->get('int')->maxDefinitionsLevel());
        $this->assertSame(0, $schema->get('int')->maxRepetitionsLevel());
        $this->assertSame(2, $schema->get('nested.int')->maxDefinitionsLevel());
        $this->assertSame(0, $schema->get('nested.int')->maxRepetitionsLevel());
        $this->assertSame(3, $schema->get('nested.nested.bool')->maxDefinitionsLevel());
        $this->assertSame(0, $schema->get('nested.nested.bool')->maxRepetitionsLevel());
        $this->assertSame(4, $schema->get('nested.list_of_ints.list.element')->maxDefinitionsLevel());
        $this->assertSame(1, $schema->get('nested.list_of_ints.list.element')->maxRepetitionsLevel());
    }

    public function test_converting_schema_to_thrift() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('int'),
            NestedColumn::struct(
                'nested',
                [
                    FlatColumn::int32('int'),
                    FlatColumn::string('strings'),
                    NestedColumn::struct(
                        'nested',
                        [
                            FlatColumn::boolean('bool'),
                        ]
                    ),
                    NestedColumn::list('list_of_ints', ListElement::int32()),
                ]
            ),
        );
        $this->assertCount(10, $schema->toThrift());
        $this->assertSame(
            [
                'schema',
                'int',
                'nested',
                'int',
                'strings',
                'nested',
                'bool',
                'list_of_ints',
                'list',
                'element',
            ],
            \array_map(static fn (SchemaElement $e) => $e->name, $schema->toThrift())
        );
    }

    public function test_flattening_schema_to_receive_simple_array_of_flat_columns() : void
    {
        $schema = Schema::with(
            NestedColumn::struct('struct_deeply_nested', [
                NestedColumn::struct('struct_0', [
                    FlatColumn::int32('int'),
                    NestedColumn::struct('struct_1', [
                        FlatColumn::string('string'),
                        NestedColumn::struct('struct_2', [
                            FlatColumn::boolean('bool'),
                            NestedColumn::struct('struct_3', [
                                FlatColumn::float('float'),
                                NestedColumn::struct('struct_4', [
                                    FlatColumn::string('string'),
                                    FlatColumn::json('json'),
                                ]),
                            ]),
                        ]),
                    ]),
                ]),
            ])
        );

        foreach ($schema->columnsFlat() as $column) {
            $this->assertInstanceOf(FlatColumn::class, $column);
        }
    }

    public function test_narrowing_schema_with_union_types() : void
    {
        $schema = \Flow\ETL\DSL\schema(
            int_schema('id'),
            union_schema('tracking_number', [StringEntry::class, IntegerEntry::class, NullEntry::class]),
        )->narrow();

        $this->assertEquals(
            \Flow\ETL\DSL\schema(
                int_schema('id'),
                str_schema('tracking_number', true),
            ),
            $schema
        );
    }
}
