<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\Parquet\IO\Extractor\PhpParquetOpenSource;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Reader;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function range;

final class PhpParquetOpenSourceTest extends FlowTestCase
{
    /**
     * @param list<int> $ids
     */
    #[TestWith([0, null, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])]
    #[TestWith([3, 5, [3, 4, 5, 6, 7]])]
    #[TestWith([6, null, [6, 7, 8, 9]])]
    #[TestWith([0, 1, [0]])]
    public function test_a_planned_window_crosses_row_groups(int $offset, ?int $limit, array $ids): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);

        $read = [];

        foreach ((new PhpParquetOpenSource((new Reader(
            engine: new PhpParquetEngine(),
        ))->readStream($filesystem->readFrom(path('memory://groups.parquet')))))->batches(
            schema(int_schema('id')),
            3,
            ParquetFilesContext::plan($filesystem, 'memory://groups.parquet', $offset, $limit),
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

        foreach ($source->batches(
            schema(str_schema('name')),
            100,
            ParquetFilesContext::plan($filesystem, 'memory://groups.parquet'),
            new PhpBackend(),
        ) as $rows) {
            static::assertEquals(schema(str_schema('name')), $rows->schema());
            $names = [...$names, ...$rows->column('name')->values()];
        }

        $source->close();

        static::assertSame(array_map(static fn(int $id): string => 'n' . $id, range(0, 9)), $names);
    }

    public function test_a_column_the_file_lacks_is_refused(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Parquet file has no column "missing"');

        iterator_to_array((new PhpParquetOpenSource((new Reader(
            engine: new PhpParquetEngine(),
        ))->readStream($filesystem->readFrom(path('memory://groups.parquet')))))->batches(
            schema(int_schema('missing')),
            10,
            ParquetFilesContext::plan($filesystem, 'memory://groups.parquet'),
            new PhpBackend(),
        ));
    }

    public function test_a_batch_size_below_one_is_refused(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Parquet batch size must be greater than 0');

        iterator_to_array((new PhpParquetOpenSource((new Reader(
            engine: new PhpParquetEngine(),
        ))->readStream($filesystem->readFrom(path('memory://groups.parquet')))))->batches(
            schema(int_schema('id')),
            0,
            ParquetFilesContext::plan($filesystem, 'memory://groups.parquet'),
            new PhpBackend(),
        ));
    }
}
