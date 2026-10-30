<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\File\FileColumns;
use Flow\ETL\IO\Extractor\File\SourceFile;
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
            ['year' => null],
            $fileColumns
                ->forFile(
                    new SourceFile(path('memory://orders/year=__HIVE_DEFAULT_PARTITION__/data.csv')),
                    $fileColumns->declare(schema()),
                )
                ->values(),
        );
    }

    public function test_a_partition_missing_from_the_path_stays_null(): void
    {
        $fileColumns = FileColumnsContext::discovering(names: ['year' => true]);

        static::assertSame(
            ['year' => null],
            $fileColumns
                ->forFile(new SourceFile(path('memory://orders/data.csv')), $fileColumns->declare(schema()))
                ->values(),
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

    public function test_a_missing_partition_under_a_not_null_declaration_is_null(): void
    {
        $fileColumns = FileColumnsContext::discovering(names: ['year' => true]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Partition column "year" of file "memory://orders/data.csv" declares type string, '
            . 'but its path value NULL cannot be cast to it.',
        );

        $fileColumns->forFile(
            new SourceFile(path('memory://orders/data.csv')),
            $fileColumns->declare(schema(str_schema('year'))),
        );
    }

    public function test_a_hive_null_value_under_a_not_null_declaration_is_null(): void
    {
        $fileColumns = FileColumnsContext::discovering(names: ['year' => true]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Partition column "year" of file "memory://orders/year=__HIVE_DEFAULT_PARTITION__/data.csv" declares '
            . 'type string, but its path value NULL cannot be cast to it.',
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
                ->names(),
        );
    }

    public function test_forFile_types_the_value_with_the_declared_partition_type(): void
    {
        $fileColumns = FileColumnsContext::discovering(types: partition_types(year: type_integer()));

        static::assertSame(
            ['year' => 2024],
            $fileColumns
                ->forFile(new SourceFile(path('memory://orders/year=2024/data.csv')), $fileColumns->declare(schema()))
                ->values(),
        );
    }

    public function test_forFile_without_a_declared_type_leaves_the_path_value_a_string(): void
    {
        $fileColumns = FileColumnsContext::discovering();

        static::assertSame(
            ['year' => '2024'],
            $fileColumns
                ->forFile(new SourceFile(path('memory://orders/year=2024/data.csv')), $fileColumns->declare(schema()))
                ->values(),
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
            ['year' => 2024],
            $fileColumns
                ->forFile(
                    new SourceFile(path('memory://orders/year=2024/data.csv')),
                    $fileColumns->declare(schema(str_schema('name'), int_schema('year'))),
                )
                ->values(),
        );
    }

    public function test_partitions_is_empty_without_partition_names(): void
    {
        static::assertEquals(
            schema(),
            FileColumnsContext::discovering(names: [], metadataColumns: true)->partitions(schema(str_schema('name'))),
        );
    }

    public function test_partitions_is_only_the_partition_block_typed_as_declared(): void
    {
        static::assertEquals(
            schema(int_schema('year'), str_schema('month', nullable: true)),
            FileColumnsContext::discovering(names: [
                'year' => false,
                'month' => true,
            ], metadataColumns: true)->partitions(schema(str_schema('name'), int_schema('year'))),
        );
    }

    public function test_partitions_uses_the_declared_partition_type_when_the_schema_has_none(): void
    {
        static::assertEquals(
            schema(int_schema('year')),
            FileColumnsContext::discovering(types: partition_types(year: type_integer()))->partitions(schema()),
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

    public function test_normalize_round_trips_names_types_and_the_metadata_flag(): void
    {
        $fileColumns = FileColumnsContext::discovering(
            ['year' => true, 'region' => false],
            partition_types(year: type_integer()),
            true,
        );

        static::assertSame(
            [
                'partition_names' => ['year' => true, 'region' => false],
                'partition_types' => ['year' => type_integer()->normalize()],
                'metadata_columns' => true,
            ],
            $fileColumns->normalize(),
        );
        static::assertEquals($fileColumns, FileColumns::fromArray($fileColumns->normalize()));
        static::assertEquals(
            $fileColumns->declare(schema(str_schema('name'))),
            FileColumns::fromArray($fileColumns->normalize())->declare(schema(str_schema('name'))),
        );
    }

    public function test_a_column_the_schema_already_declares_is_left_alone(): void
    {
        // an explicitly declared partition column keeps the type and nullability the caller chose
        static::assertEquals(
            schema(int_schema('date')),
            FileColumnsContext::discovering(['date' => true])->declare(schema(int_schema('date'))),
        );
    }

    public function test_declare_adds_each_name_with_the_nullability_it_was_given(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('date'), str_schema('region', nullable: true)),
            FileColumnsContext::discovering(['date' => false, 'region' => true])->declare(schema(int_schema('id'))),
        );
    }

    public function test_declare_moves_a_partition_column_out_of_its_body_position_and_keeps_its_type(): void
    {
        // the file's own header puts `group` first; the partition block is appended, so the declared
        // int type survives but the position does not
        static::assertEquals(
            schema(int_schema('id'), str_schema('value'), int_schema('group')),
            FileColumnsContext::discovering(['group' => false])->declare(schema(
                int_schema('group'),
                int_schema('id'),
                str_schema('value'),
            )),
        );
    }

    public function test_declare_leaves_a_schema_alone_when_nothing_was_discovered(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            FileColumnsContext::discovering([])->declare(schema(int_schema('id'))),
        );
    }
}
