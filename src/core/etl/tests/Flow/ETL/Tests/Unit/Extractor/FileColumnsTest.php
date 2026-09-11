<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Tests\Context\FileColumnsContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_types;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_integer;

final class FileColumnsTest extends FlowTestCase
{
    public function test_a_hive_null_partition_value_stays_null(): void
    {
        $fileColumns = FileColumnsContext::discovering(names: ['year' => true]);

        static::assertSame(
            ['name' => 'Norbert', 'year' => null],
            $fileColumns
                ->forFile(
                    new SourceFile(path('memory://orders/year=__HIVE_DEFAULT_PARTITION__/data.csv')),
                    $fileColumns->declare(schema()),
                )
                ->fill(['name' => 'Norbert']),
        );
    }

    public function test_a_partition_missing_from_the_path_stays_null(): void
    {
        $fileColumns = FileColumnsContext::discovering(names: ['year' => true]);

        static::assertSame(
            ['name' => 'Norbert', 'year' => null],
            $fileColumns
                ->forFile(new SourceFile(path('memory://orders/data.csv')), $fileColumns->declare(schema()))
                ->fill(['name' => 'Norbert']),
        );
    }

    public function test_an_uncastable_partition_value_names_the_file_and_the_column(): void
    {
        $fileColumns = FileColumnsContext::discovering(types: partition_types(year: type_integer()));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Partition column "year" of file "memory://orders/year=abc/part-0.parquet" declares type integer, '
            . 'but its path value "abc" cannot be cast to it.',
        );

        $fileColumns->forFile(
            new SourceFile(path('memory://orders/year=abc/part-0.parquet')),
            $fileColumns->declare(schema()),
        );
    }

    public function test_a_hive_null_value_under_a_not_null_declaration_names_the_sentinel(): void
    {
        $fileColumns = FileColumnsContext::discovering(names: ['year' => true]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Partition column "year" of file "memory://orders/year=__HIVE_DEFAULT_PARTITION__/data.csv" declares '
            . 'type string, but its path value "__HIVE_DEFAULT_PARTITION__" cannot be cast to it.',
        );

        $fileColumns->forFile(
            new SourceFile(path('memory://orders/year=__HIVE_DEFAULT_PARTITION__/data.csv')),
            $fileColumns->declare(schema(str_schema('year'))),
        );
    }

    public function test_declare_appends_the_metadata_column_then_the_partition_block(): void
    {
        static::assertSame(
            ['name', '_input_file_uri', 'year'],
            FileColumnsContext::discovering(metadataColumns: true)
                ->declare(schema(str_schema('name'), str_schema('year')))
                ->references()
                ->names(),
        );
    }

    public function test_forFile_types_the_value_with_the_declared_partition_type(): void
    {
        $fileColumns = FileColumnsContext::discovering(types: partition_types(year: type_integer()));

        static::assertSame(
            ['name' => 'Norbert', 'year' => 2024],
            $fileColumns
                ->forFile(new SourceFile(path('memory://orders/year=2024/data.csv')), $fileColumns->declare(schema()))
                ->fill(['name' => 'Norbert']),
        );
    }

    public function test_forFile_without_a_declared_type_leaves_the_path_value_a_string(): void
    {
        $fileColumns = FileColumnsContext::discovering();

        static::assertSame(
            ['name' => 'Norbert', 'year' => '2024'],
            $fileColumns
                ->forFile(new SourceFile(path('memory://orders/year=2024/data.csv')), $fileColumns->declare(schema()))
                ->fill(['name' => 'Norbert']),
        );
    }

    public function test_the_declared_schema_beats_a_declared_partition_type(): void
    {
        static::assertEquals(
            datetime_schema('year'),
            FileColumnsContext::discovering(types: partition_types(year: type_integer()))->declare(schema(
                str_schema('name'),
                datetime_schema('year'),
            ))->get('year'),
        );
    }

    public function test_the_declared_schema_types_the_value_forFile_produces(): void
    {
        $fileColumns = FileColumnsContext::discovering();

        static::assertSame(
            ['name' => 'Norbert', 'year' => 2024],
            $fileColumns
                ->forFile(
                    new SourceFile(path('memory://orders/year=2024/data.csv')),
                    $fileColumns->declare(schema(str_schema('name'), int_schema('year'))),
                )
                ->fill(['name' => 'Norbert']),
        );
    }

    public function test_without_tail_ignores_names_the_schema_does_not_carry(): void
    {
        static::assertTrue(schema(int_schema('id'), str_schema('name'))->isSame(FileColumnsContext::discovering(names: [
            'group' => false,
        ])->withoutTail(schema(int_schema('id'), str_schema('name')))));
    }

    public function test_without_tail_is_an_identity_copy_when_there_is_no_tail(): void
    {
        static::assertTrue(schema(int_schema('id'), str_schema('name'))->isSame(FileColumnsContext::discovering(
            names: [],
            metadataColumns: false,
        )->withoutTail(schema(int_schema('id'), str_schema('name')))));
    }

    public function test_without_tail_removes_a_partition_named_body_column(): void
    {
        static::assertTrue(
            schema(int_schema('id'))
                ->isSame(FileColumnsContext::discovering(names: ['group' => false])->withoutTail(schema(
                    str_schema('group'),
                    int_schema('id'),
                ))),
        );
    }

    public function test_without_tail_removes_the_metadata_column(): void
    {
        static::assertTrue(
            schema(str_schema('name'))
                ->isSame(FileColumnsContext::discovering(names: [], metadataColumns: true)->withoutTail(schema(
                    str_schema('_input_file_uri'),
                    str_schema('name'),
                ))),
        );
    }
}
