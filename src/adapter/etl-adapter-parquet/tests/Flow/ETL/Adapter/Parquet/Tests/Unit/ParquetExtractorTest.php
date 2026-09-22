<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Closure;
use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\ETL\Cardinality;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Options;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class ParquetExtractorTest extends FlowTestCase
{
    public function test_a_glob_extrapolates_from_the_first_footer(): void
    {
        $statistics = from_parquet(__DIR__ . '/../Integration/Fixtures/Pagination/*.parquet')->statistics();

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
            from_parquet(__DIR__ . '/../Integration/Fixtures/Pagination/01_1000.parquet')->statistics()->rows,
        );
    }

    public function test_a_single_file_declares_the_uncompressed_byte_size(): void
    {
        static::assertEquals(
            Cardinality::exact(15934),
            from_parquet(__DIR__ . '/../Integration/Fixtures/Pagination/01_1000.parquet')->statistics()->size,
        );
    }

    public function test_an_offset_is_subtracted_from_the_row_count(): void
    {
        static::assertEquals(
            Cardinality::exact(900),
            from_parquet(
                __DIR__ . '/../Integration/Fixtures/Pagination/01_1000.parquet',
                offset: 100,
            )->statistics()->rows,
        );
    }

    public function test_an_offset_larger_than_the_file_declares_zero_rows(): void
    {
        static::assertEquals(
            Cardinality::exact(0),
            from_parquet(
                __DIR__ . '/../Integration/Fixtures/Pagination/01_1000.parquet',
                offset: 1_000_000_000,
            )->statistics()->rows,
        );
    }

    public function test_changing_the_offset_invalidates_the_memo(): void
    {
        $extractor = from_parquet(__DIR__ . '/../Integration/Fixtures/Pagination/01_1000.parquet');

        static::assertEquals(Cardinality::exact(1000), $extractor->statistics()->rows);
        static::assertEquals(Cardinality::exact(750), $extractor->withOffset(250)->statistics()->rows);
    }

    public function test_statistics_are_computed_at_most_once(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(
            __DIR__ . '/../Integration/Fixtures/Pagination/01_1000.parquet',
            filesystem: $filesystem,
        );

        static::assertSame($extractor->statistics(), $extractor->statistics());
        static::assertSame(1, $filesystem->readFromCalls);
        static::assertSame(1, $filesystem->closedStreams());
    }

    public function test_union_by_name_sums_every_footer_exactly(): void
    {
        $statistics = from_parquet(__DIR__ . '/../Integration/Fixtures/Pagination/*.parquet')
            ->unionByName()
            ->statistics();

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
        $extractor = from_parquet(
            __DIR__ . '/../Integration/Fixtures/Pagination/01_1000.parquet',
            filesystem: $filesystem,
        );

        $extractor->statistics();
        $setter($extractor);
        $extractor->statistics();

        static::assertSame(2, $filesystem->readFromCalls);
    }

    public function test_a_new_offset_reuses_the_footer_already_read(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(
            __DIR__ . '/../Integration/Fixtures/Pagination/01_1000.parquet',
            filesystem: $filesystem,
        );

        $extractor->statistics();
        $extractor->withOffset(100);

        static::assertEquals(Cardinality::exact(900), $extractor->statistics()->rows);
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_schema_then_statistics_reads_the_footer_once(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(
            __DIR__ . '/../Integration/Fixtures/Pagination/01_1000.parquet',
            filesystem: $filesystem,
        );

        $extractor->schema();
        $extractor->statistics();

        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_a_declared_schema_still_reads_the_footer_for_statistics(): void
    {
        static::assertEquals(
            Cardinality::exact(1000),
            from_parquet(__DIR__ . '/../Integration/Fixtures/Pagination/01_1000.parquet')
                ->withSchema(schema(int_schema('id')))
                ->statistics()
                ->rows,
        );
    }
}
