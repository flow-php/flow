<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\PartitionColumns;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\MemoryFilesystem;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;

final class PartitionColumnsTest extends FlowTestCase
{
    public function test_a_column_the_schema_already_declares_is_left_alone(): void
    {
        // an explicitly declared partition column keeps the type and nullability the caller chose
        static::assertEquals(
            schema(int_schema('date')),
            (new PartitionColumns(new MemoryFilesystem()))->declare(schema(int_schema('date')), ['date' => true]),
        );
    }

    public function test_apply_adds_a_column_the_inferred_schema_is_missing(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('date', nullable: true)),
            (new PartitionColumns(new MemoryFilesystem()))
                ->apply(rows(schema(int_schema('id')), row(['id' => 1])), ['date' => true])
                ->schema(),
        );
    }

    public function test_apply_keeps_the_rows_it_was_given(): void
    {
        static::assertEquals(
            [row(['id' => 1, 'date' => null])],
            (new PartitionColumns(new MemoryFilesystem()))->apply(
                rows(schema(int_schema('id'), str_schema('date', nullable: true)), row(['id' => 1, 'date' => null])),
                ['date' => true],
            )->all(),
        );
    }

    public function test_apply_replaces_the_definition_inference_guessed(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('date', nullable: true)),
            (new PartitionColumns(new MemoryFilesystem()))
                ->apply(rows(schema(int_schema('id'), str_schema('date')), row(['id' => 1, 'date' => '2026-01-01'])), [
                    'date' => true,
                ])
                ->schema(),
        );
    }

    public function test_apply_takes_nullability_from_the_names_map(): void
    {
        static::assertEquals(
            schema(str_schema('date'), str_schema('region', nullable: true)),
            (new PartitionColumns(new MemoryFilesystem()))
                ->apply(rows(schema(str_schema('date', nullable: true)), row(['date' => '2026-01-01'])), [
                    'date' => false,
                    'region' => true,
                ])
                ->schema(),
        );
    }

    public function test_declare_adds_each_name_with_the_nullability_it_was_given(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('date'), str_schema('region', nullable: true)),
            (new PartitionColumns(new MemoryFilesystem()))->declare(schema(int_schema('id')), [
                'date' => false,
                'region' => true,
            ]),
        );
    }

    public function test_fill_leaves_a_row_untouched_when_there_are_no_partition_columns(): void
    {
        static::assertSame(['id' => 1], (new PartitionColumns(new MemoryFilesystem()))->fill(['id' => 1], [], []));
    }

    public function test_fill_nulls_a_column_the_path_does_not_carry(): void
    {
        static::assertSame(
            ['id' => 1, 'date' => '2026-01-01', 'region' => null],
            (new PartitionColumns(new MemoryFilesystem()))->fill(
                ['id' => 1],
                ['date' => false, 'region' => true],
                ['date' => '2026-01-01'],
            ),
        );
    }

    public function test_names_of_a_listing_without_partitions_is_empty(): void
    {
        $filesystem = new MemoryFilesystem();
        $filesystem->appendTo(path('memory://plain/data.csv'))->append('id')->close();

        static::assertSame(
            [],
            (new PartitionColumns($filesystem))->names(path('memory://plain/*.csv'), new OnlyFiles()),
        );
    }

    public function test_names_reports_a_partition_every_path_carries_as_non_nullable(): void
    {
        $filesystem = new MemoryFilesystem();
        $filesystem->appendTo(path('memory://all/date=2026-01-01/data.csv'))->append('id')->close();
        $filesystem->appendTo(path('memory://all/date=2026-01-02/data.csv'))->append('id')->close();

        static::assertSame(
            ['date' => false],
            (new PartitionColumns($filesystem))->names(path('memory://all/*/*.csv'), new OnlyFiles()),
        );
    }

    public function test_names_reports_a_partition_only_some_paths_carry_as_nullable(): void
    {
        $filesystem = new MemoryFilesystem();
        $filesystem->appendTo(path('memory://mixed/date=2026-01-01/data.csv'))->append('id')->close();
        $filesystem->appendTo(path('memory://mixed/plain/data.csv'))->append('id')->close();

        static::assertSame(
            ['date' => true],
            (new PartitionColumns($filesystem))->names(path('memory://mixed/*/*.csv'), new OnlyFiles()),
        );
    }
}
