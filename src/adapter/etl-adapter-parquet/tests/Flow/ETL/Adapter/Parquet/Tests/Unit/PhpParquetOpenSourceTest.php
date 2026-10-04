<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\PhpParquetOpenSource;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Reader;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function range;

final class PhpParquetOpenSourceTest extends FlowTestCase
{
    /**
     * @param list<int> $ids
     */
    #[TestWith([null, null, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])]
    #[TestWith([3, 5, [3, 4, 5, 6, 7]])]
    #[TestWith([6, null, [6, 7, 8, 9]])]
    #[TestWith([0, 1, [0]])]
    public function test_offset_and_limit_cross_row_groups(?int $offset, ?int $limit, array $ids): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);

        $read = [];

        foreach ((new PhpParquetOpenSource((new Reader(
            engine: new PhpParquetEngine(),
        ))->readStream($filesystem->readFrom(path('memory://groups.parquet')))))->batches(
            schema(int_schema('id')),
            3,
            $offset,
            $limit,
            new PhpBackend(),
        ) as $rows) {
            static::assertLessThanOrEqual(3, $rows->count());
            $read = [...$read, ...$rows->column('id')->values()];
        }

        static::assertSame($ids, $read);
    }

    public function test_batches_hold_the_schema_columns(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);
        $source = new PhpParquetOpenSource((new Reader(
            engine: new PhpParquetEngine(),
        ))->readStream($filesystem->readFrom(path('memory://groups.parquet'))));

        $names = [];

        foreach ($source->batches(schema(str_schema('name')), 100, null, null, new PhpBackend()) as $rows) {
            static::assertEquals(schema(str_schema('name')), $rows->schema());
            $names = [...$names, ...$rows->column('name')->values()];
        }

        $source->close();

        static::assertSame(array_map(static fn(int $id): string => 'n' . $id, range(0, 9)), $names);
    }
}
