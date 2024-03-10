<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile;

use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
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

        self::assertSame(1, $schema->get('int')->maxDefinitionsLevel());
        self::assertSame(0, $schema->get('int')->maxRepetitionsLevel());
        self::assertSame(2, $schema->get('nested.int')->maxDefinitionsLevel());
        self::assertSame(0, $schema->get('nested.int')->maxRepetitionsLevel());
        self::assertSame(3, $schema->get('nested.nested.bool')->maxDefinitionsLevel());
        self::assertSame(0, $schema->get('nested.nested.bool')->maxRepetitionsLevel());
        self::assertSame(4, $schema->get('nested.list_of_ints.list.element')->maxDefinitionsLevel());
        self::assertSame(1, $schema->get('nested.list_of_ints.list.element')->maxRepetitionsLevel());
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
        self::assertCount(10, $schema->toThrift());
        self::assertSame(
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
            self::assertInstanceOf(FlatColumn::class, $column);
        }
    }
}
