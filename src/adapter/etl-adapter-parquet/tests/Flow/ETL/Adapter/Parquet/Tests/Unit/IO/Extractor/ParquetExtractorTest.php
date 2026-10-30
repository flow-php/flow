<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\IO\Extractor;

use Closure;
use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\Planning\Logical\Trigger;
use Flow\ETL\Planning\Optimizer\Rule\CountFromStatistics;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Exception\InvalidArgumentException as NotParquet;
use Flow\Parquet\Options;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function gc_collect_cycles;

final class ParquetExtractorTest extends FlowTestCase
{
    public function test_a_glob_extrapolates_from_the_first_footer(): void
    {
        $statistics = from_parquet(__DIR__ . '/../../../Fixtures/Pagination/*.parquet')->statistics();

        static::assertEquals(
            Cardinality::approximately(5 * 1000, Cardinality::DEFAULT_RELATIVE_ERROR),
            $statistics->rows,
        );
        static::assertEquals(
            Cardinality::approximately(5 * 15934, Cardinality::DEFAULT_RELATIVE_ERROR),
            $statistics->size,
        );
    }

    public function test_a_single_file_declares_the_footer_row_count(): void
    {
        static::assertEquals(
            Cardinality::exact(1000),
            from_parquet(__DIR__ . '/../../../Fixtures/Pagination/01_1000.parquet')->statistics()->rows,
        );
    }

    public function test_a_single_file_declares_the_uncompressed_byte_size(): void
    {
        static::assertEquals(
            Cardinality::exact(15934),
            from_parquet(__DIR__ . '/../../../Fixtures/Pagination/01_1000.parquet')->statistics()->size,
        );
    }

    public function test_statistics_are_computed_at_most_once(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(__DIR__ . '/../../../Fixtures/Pagination/01_1000.parquet', filesystem: $filesystem);

        static::assertSame($extractor->statistics(), $extractor->statistics());
        static::assertSame(1, $filesystem->readFromCalls);
        static::assertSame(0, $filesystem->closedStreams());

        unset($extractor);
        gc_collect_cycles();

        static::assertSame(1, $filesystem->closedStreams());
    }

    public function test_a_file_that_is_not_parquet_closes_the_stream_it_opened(): void
    {
        $filesystem = new CountingFilesystem(memory_filesystem());
        $filesystem->writeTo(path('memory://not.parquet'))->append('id,name')->close();

        try {
            from_parquet(path('memory://not.parquet'), filesystem: $filesystem)->statistics();
            static::fail('a CSV file was read as Parquet');
        } catch (NotParquet) {
        }

        static::assertSame(1, $filesystem->readFromCalls);
        static::assertSame(1, $filesystem->closedStreams());
    }

    public function test_union_by_name_sums_every_footer_exactly(): void
    {
        $statistics = from_parquet(__DIR__ . '/../../../Fixtures/Pagination/*.parquet')->unionByName()->statistics();

        static::assertEquals(Cardinality::exact(1000 + 500 + 350 + 2000 + 15), $statistics->rows);
        static::assertEquals(Cardinality::exact(15934 + 7932 + 5532 + 32935 + 252), $statistics->size);
    }

    /**
     * @return Generator<string, array{Closure(ParquetExtractor): mixed}>
     */
    public static function memo_resetting_setters(): Generator
    {
        yield 'unionByName' => [static fn(ParquetExtractor $e): mixed => $e->unionByName()];
        yield 'withByteOrder' => [static fn(ParquetExtractor $e): mixed => $e->withByteOrder(ByteOrder::LITTLE_ENDIAN)];
        yield 'withColumns' => [static fn(ParquetExtractor $e): mixed => $e->withColumns(['id'])];
        yield 'withEngine' => [static fn(ParquetExtractor $e): mixed => $e->withEngine(null)];
        yield 'withOptions' => [static fn(ParquetExtractor $e): mixed => $e->withOptions(Options::default())];
    }

    #[DataProvider('memo_resetting_setters')]
    public function test_a_setter_reads_the_footer_again(Closure $setter): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(__DIR__ . '/../../../Fixtures/Pagination/01_1000.parquet', filesystem: $filesystem);

        $extractor->statistics();
        $setter($extractor);
        $extractor->statistics();

        static::assertSame(2, $filesystem->readFromCalls);
    }

    public function test_schema_then_statistics_reads_the_footer_once(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(__DIR__ . '/../../../Fixtures/Pagination/01_1000.parquet', filesystem: $filesystem);

        $extractor->schema();
        $extractor->statistics();

        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_a_declared_schema_answers_without_opening_a_file(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            from_parquet(path('memory://no-match/*.parquet'), filesystem: memory_filesystem())
                ->withSchema(schema(int_schema('id')))
                ->schema(),
        );
    }

    public function test_extract_builds_through_the_config_backend(): void
    {
        $backend = new SpyBackend();

        from_parquet(__DIR__ . '/../../../Fixtures/Pagination/01_1000.parquet')
            ->extract(flow_context(config_builder()->backend($backend)->build())->backend(), new Criteria())
            ->current();

        static::assertGreaterThanOrEqual(1, $backend->builders() + $backend->adopts());
    }

    public function test_a_count_over_a_plain_read_reads_the_number_from_the_footer(): void
    {
        $memory = memory_filesystem();
        df()
            ->read(from_sequence_number('id', 1, 25))
            ->write(to_parquet(path('memory://count/file.parquet'), filesystem: $memory))
            ->run();

        static::assertEquals(
            from_rows(array_to_rows([['count' => 25]], schema(int_schema('count')))),
            (new CountFromStatistics())
                ->apply(
                    df()
                        ->read(from_parquet(path('memory://count/file.parquet'), filesystem: $memory))
                        ->explain(Trigger::count)
                        ->logical,
                    config()->backend(),
                )
                ->source()
                ->extractor(),
        );
    }
}
