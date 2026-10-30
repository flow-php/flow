<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\IO\Extractor\File\FileColumnsSource;
use Flow\ETL\IO\Extractor\File\PartitionTypes;
use Flow\ETL\IO\Extractor\File\SourceFiles;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\partition_types;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_integer;

final class FileColumnsSourceTest extends FlowTestCase
{
    public function test_columns_name_the_partitions_of_the_listed_files(): void
    {
        $source = new FileColumnsSource(
            new SourceFiles(MemoryFiles::with([
                'memory://orders/year=2024/a.csv' => 'id',
                'memory://orders/year=2025/b.csv' => 'id',
            ]), path('memory://orders/*/*.csv')),
        );

        static::assertSame(
            ['partition_names' => ['year' => false], 'partition_types' => [], 'metadata_columns' => false],
            $source->columns()->normalize(),
        );
    }

    public function test_the_withers_share_the_listing(): void
    {
        $counting = new CountingFilesystem(MemoryFiles::with(['memory://orders/year=2024/a.csv' => 'id']));
        $source = new FileColumnsSource(new SourceFiles($counting, path('memory://orders/*/*.csv')));
        $source->columns();

        $source->withPartitionTypes(partition_types(year: type_integer()))->withMetadataColumns(true)->columns();

        static::assertSame(1, $counting->listCalls);
    }

    public function test_the_columns_carry_the_declared_partition_types_and_the_metadata_flag(): void
    {
        $source = (new FileColumnsSource(
            new SourceFiles(MemoryFiles::with([
                'memory://orders/year=2024/a.csv' => 'id',
            ]), path('memory://orders/*/*.csv')),
        ))
            ->withPartitionTypes(partition_types(year: type_integer()))
            ->withMetadataColumns(true);

        static::assertEquals(
            [
                'partition_names' => ['year' => false],
                'partition_types' => partition_types(year: type_integer())->normalize(),
                'metadata_columns' => true,
            ],
            $source->columns()->normalize(),
        );
    }

    public function test_no_partition_type_is_declared_by_default(): void
    {
        static::assertEquals(
            (new PartitionTypes())->normalize(),
            (new FileColumnsSource(new SourceFiles(MemoryFiles::with([]), path('memory://orders/*.csv'))))
                ->columns()
                ->normalize()['partition_types'],
        );
    }

    public function test_a_wither_leaves_the_source_it_was_called_on_unchanged(): void
    {
        $source = new FileColumnsSource(new SourceFiles(MemoryFiles::with([]), path('memory://orders/*.csv')));

        $source->withMetadataColumns(true)->withPartitionTypes(partition_types(year: type_integer()));

        static::assertSame(
            ['partition_names' => [], 'partition_types' => [], 'metadata_columns' => false],
            $source->columns()->normalize(),
        );
    }
}
