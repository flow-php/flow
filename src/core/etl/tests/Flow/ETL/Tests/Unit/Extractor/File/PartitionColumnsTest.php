<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\File;

use Flow\ETL\Extractor\File\PartitionColumns;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\MemoryFilesystem;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function Flow\ETL\DSL\int_schema;
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

    public function test_declare_moves_a_partition_column_out_of_its_body_position_and_keeps_its_type(): void
    {
        // the file's own header puts `group` first; the partition block is appended, so the declared
        // int type survives but the position does not
        static::assertEquals(
            schema(int_schema('id'), str_schema('value'), int_schema('group')),
            (new PartitionColumns(new MemoryFilesystem()))->declare(
                schema(int_schema('group'), int_schema('id'), str_schema('value')),
                ['group' => false],
            ),
        );
    }

    public function test_declare_leaves_a_schema_alone_when_nothing_was_discovered(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            (new PartitionColumns(new MemoryFilesystem()))->declare(schema(int_schema('id')), []),
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

    public function test_names_are_sorted_by_name_not_by_path_order(): void
    {
        $filesystem = new MemoryFilesystem();
        $filesystem->appendTo(path('memory://sorted/year=2026/month=01/region=eu/data.csv'))->append('id')->close();

        static::assertSame(
            ['month' => false, 'region' => false, 'year' => false],
            (new PartitionColumns($filesystem))->names(path('memory://sorted/*/*/*/*.csv'), new OnlyFiles()),
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
