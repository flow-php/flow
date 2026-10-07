<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\KeepPaths;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_sum;
use function array_values;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function gc_collect_cycles;
use function iterator_to_array;

final class ParquetExtractorFooterTest extends FlowTestCase
{
    public const string PAGINATION = __DIR__ . '/Fixtures/Pagination';

    public function test_one_file_is_opened_once_per_read(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        data_frame()->read(from_parquet(self::PAGINATION . '/01_1000.parquet', filesystem: $filesystem))->fetch();

        static::assertSame(
            [self::PAGINATION . '/01_1000.parquet' => 1],
            ParquetFilesContext::opensPerFile($filesystem),
        );
        static::assertSame(1, $filesystem->closedStreams());
    }

    public function test_every_file_is_opened_once_even_after_schema(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(path(self::PAGINATION . '/*.parquet'), filesystem: $filesystem);

        $extractor->schema();
        iterator_to_array($extractor->extract(flow_context()));

        static::assertSame([1, 1, 1, 1, 1], array_values(ParquetFilesContext::opensPerFile($filesystem)));
        static::assertSame(5, $filesystem->closedStreams());
    }

    public function test_changing_the_columns_closes_the_kept_file_and_opens_it_once_more(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(path(self::PAGINATION . '/*.parquet'), filesystem: $filesystem);

        $extractor->schema();
        $extractor->withColumns(['id']);

        static::assertSame(1, $filesystem->closedStreams());

        iterator_to_array($extractor->extract(flow_context()));

        static::assertSame([2, 1, 1, 1, 1], array_values(ParquetFilesContext::opensPerFile($filesystem)));
        static::assertSame(6, $filesystem->closedStreams());
    }

    public function test_a_second_extract_opens_every_file_once_more(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(path(self::PAGINATION . '/*.parquet'), filesystem: $filesystem);

        iterator_to_array($extractor->extract(flow_context()));
        iterator_to_array($extractor->extract(flow_context()));

        static::assertSame([2, 2, 2, 2, 2], array_values(ParquetFilesContext::opensPerFile($filesystem)));
        static::assertSame(10, $filesystem->closedStreams());
    }

    public function test_the_kept_file_is_closed_when_the_extractor_is_released_unread(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(path(self::PAGINATION . '/*.parquet'), filesystem: $filesystem);

        $extractor->schema();

        static::assertSame(0, $filesystem->closedStreams());

        unset($extractor);
        gc_collect_cycles();

        static::assertSame(1, $filesystem->readFromCalls);
        static::assertSame(1, $filesystem->closedStreams());
    }

    public function test_a_path_filter_excluding_the_first_file_closes_the_kept_one(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(path(self::PAGINATION . '/*.parquet'), filesystem: $filesystem);

        $extractor->schema();
        $rows = iterator_to_array($extractor->extract(flow_context(), pathFilter: new KeepPaths([path(self::PAGINATION
        . '/02_500.parquet')])), false);

        static::assertSame(500, array_sum(array_map(static fn(Rows $batch): int => $batch->count(), $rows)));
        static::assertSame(
            [self::PAGINATION . '/01_1000.parquet' => 1, self::PAGINATION . '/02_500.parquet' => 1],
            ParquetFilesContext::opensPerFile($filesystem),
        );
        static::assertSame(2, $filesystem->closedStreams());
    }

    public function test_a_partition_filter_excluding_the_first_file_closes_the_kept_one(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        $rows = data_frame()
            ->read(from_parquet(path(self::PAGINATION . '/partitioned/**/*.parquet'), filesystem: $filesystem))
            ->filter(ref('date')->equals(lit('2024-12-01')))
            ->fetch();

        static::assertCount(2000, $rows);
        static::assertSame(
            [
                self::PAGINATION . '/partitioned/date=2024-01-01/01_1000.parquet' => 1,
                self::PAGINATION . '/partitioned/date=2024-12-01/04_2000.parquet' => 1,
            ],
            ParquetFilesContext::opensPerFile($filesystem),
        );
        static::assertSame(2, $filesystem->closedStreams());
    }

    public static function day_range_bounds(): Generator
    {
        yield 'datetime bounds' => [
            new DateTimeImmutable('2026-10-02 00:00:00 UTC'),
            new DateTimeImmutable('2026-10-03 00:00:00 UTC'),
        ];
        yield 'string bounds' => ['2026-10-02', '2026-10-03'];
    }

    #[DataProvider('day_range_bounds')]
    public function test_a_datetime_range_over_a_partition_column_the_file_holds_opens_only_the_matching_folders(
        DateTimeImmutable|string $from,
        DateTimeImmutable|string $to,
    ): void {
        $filesystem = new CountingFilesystem(memory_filesystem());

        data_frame()
            ->read(from_array(
                [
                    ['id' => 'a', 'date' => new DateTimeImmutable('2026-10-01 00:00:00 UTC')],
                    ['id' => 'b', 'date' => new DateTimeImmutable('2026-10-02 00:00:00 UTC')],
                    ['id' => 'c', 'date' => new DateTimeImmutable('2026-10-03 00:00:00 UTC')],
                    ['id' => 'd', 'date' => new DateTimeImmutable('2026-10-04 00:00:00 UTC')],
                ],
                schema(str_schema('id'), datetime_schema('date')),
            ))
            ->write(
                to_parquet(path('memory://var/pruned/file.parquet'), filesystem: $filesystem)->partitionBy(
                    partition_by('date')->writeColumns(),
                ),
            )
            ->run();

        $rows = data_frame()
            ->read(from_parquet(path('memory://var/pruned/**/*.parquet'), filesystem: $filesystem))
            ->filter(ref('date')->between(lit($from), lit($to), Boundary::INCLUSIVE))
            ->fetch();

        static::assertSame(['b', 'c'], $rows->reduceToArray('id'));
        // the first listed file is opened once for the footer
        static::assertSame(
            [
                '/var/pruned/date=2026-10-01T00%3A00%3A00%2B00%3A00/file.parquet' => 1,
                '/var/pruned/date=2026-10-02T00%3A00%3A00%2B00%3A00/file.parquet' => 1,
                '/var/pruned/date=2026-10-03T00%3A00%3A00%2B00%3A00/file.parquet' => 1,
            ],
            ParquetFilesContext::opensPerFile($filesystem),
        );
    }
}
