<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\RustParquetOpenSource;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Column\Php\ScalarColumn;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Tests\Double\FailingReadSourceStream;
use Flow\Parquet\Engine\RustParquetFileReader;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function range;

#[RequiresPhpExtension('flow_php')]
#[RequiresPhpExtension('arrow')]
final class RustParquetOpenSourceTest extends FlowTestCase
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

        foreach ((new RustParquetOpenSource(new RustParquetFileReader($filesystem->readFrom(path(
            'memory://groups.parquet',
        )))))->batches(schema(int_schema('id')), 3, $offset, $limit, new AdaptiveBackend()) as $rows) {
            static::assertLessThanOrEqual(3, $rows->count());
            $read = [...$read, ...$rows->column('id')->values()];
        }

        static::assertSame($ids, $read);
    }

    public function test_columns_are_adopted_by_the_backend(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);
        $source = new RustParquetOpenSource(new RustParquetFileReader($filesystem->readFrom(path(
            'memory://groups.parquet',
        ))));

        $names = [];

        foreach ($source->batches(schema(str_schema('name')), 100, null, null, new PhpBackend()) as $rows) {
            static::assertEquals(schema(str_schema('name')), $rows->schema());
            static::assertInstanceOf(ScalarColumn::class, $rows->column('name'));
            $names = [...$names, ...$rows->column('name')->values()];
        }

        $source->close();

        static::assertSame(array_map(static fn(int $id): string => 'n' . $id, range(0, 9)), $names);
    }

    public function test_a_second_batches_call_closes_the_first_reader(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);
        $source = new RustParquetOpenSource(new RustParquetFileReader($filesystem->readFrom(path(
            'memory://groups.parquet',
        ))));
        $first = $source->batches(schema(int_schema('id')), 3, null, null, new AdaptiveBackend());
        $first->rewind();
        static::assertSame([0, 1, 2], $first->current()->column('id')->values());

        $second = [];

        foreach ($source->batches(schema(int_schema('id')), 3, null, null, new AdaptiveBackend()) as $rows) {
            $second = [...$second, ...$rows->column('id')->values()];
        }

        $first->next();

        static::assertFalse($first->valid());
        static::assertSame(range(0, 9), $second);
    }

    public function test_close_before_batches_is_harmless(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);

        (new RustParquetOpenSource(new RustParquetFileReader($filesystem->readFrom(path(
            'memory://groups.parquet',
        )))))->close();

        $this->expectNotToPerformAssertions();
    }

    public function test_an_exception_the_stream_throws_while_opening_surfaces_as_itself(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Reading "memory://groups.parquet" failed');

        new RustParquetOpenSource(new RustParquetFileReader(new FailingReadSourceStream($filesystem->readFrom(path(
            'memory://groups.parquet',
        )))));
    }
}
