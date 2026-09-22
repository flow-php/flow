<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Closure;
use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Cardinality;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function abs;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class CSVExtractorTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{Closure(CSVExtractor): mixed}>
     */
    public static function sample_dropping_setters(): Generator
    {
        yield 'inferSchema' => [static fn(CSVExtractor $e): mixed => $e->inferSchema(infer_schema())];
        yield 'withBOMRemoval' => [static fn(CSVExtractor $e): mixed => $e->withBOMRemoval(true)];
        yield 'withCharactersReadInLine' => [static fn(CSVExtractor $e): mixed => $e->withCharactersReadInLine(1024)];
        yield 'withEmptyToNull' => [static fn(CSVExtractor $e): mixed => $e->withEmptyToNull(true)];
        yield 'withEnclosure' => [static fn(CSVExtractor $e): mixed => $e->withEnclosure('"')];
        yield 'withEscape' => [static fn(CSVExtractor $e): mixed => $e->withEscape('\\')];
        yield 'withHeader' => [static fn(CSVExtractor $e): mixed => $e->withHeader(true)];
        yield 'withSeparator' => [static fn(CSVExtractor $e): mixed => $e->withSeparator(',')];
    }

    #[DataProvider('sample_dropping_setters')]
    public function test_a_changed_read_option_drops_the_sample(Closure $setter): void
    {
        $extractor = from_csv(CSVFixtureContext::path('five_rows.csv'));
        $extractor->schema();

        static::assertEquals(Cardinality::exact(5), $extractor->statistics()->rows);

        $setter($extractor);

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->rows);
    }

    public function test_a_declared_schema_declares_unknown_rows(): void
    {
        $extractor = from_csv(
            CSVFixtureContext::path('five_rows.csv'),
            schema: schema(int_schema('id'), str_schema('name')),
        );
        $extractor->schema();

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->rows);
        static::assertEquals(Cardinality::exact(28), $extractor->statistics()->size);
    }

    public function test_a_file_shorter_than_the_sample_declares_an_exact_row_count(): void
    {
        $extractor = from_csv(CSVFixtureContext::path('five_rows.csv'));
        $extractor->schema();

        static::assertEquals(Cardinality::exact(5), $extractor->statistics()->rows);
    }

    public function test_a_header_only_file_declares_zero_rows_exactly(): void
    {
        $extractor = from_csv(CSVFixtureContext::path('header_only.csv'));
        $extractor->schema();

        static::assertEquals(Cardinality::exact(0), $extractor->statistics()->rows);
        static::assertEquals(Cardinality::exact(8), $extractor->statistics()->size);
    }

    public function test_a_member_without_a_size_makes_both_facts_unknown(): void
    {
        $extractor = from_csv(CSVFixtureContext::path('glob_with_empty/*.csv'))
            ->inferSchema(infer_schema()->sampleSize(1));
        $extractor->schema();

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->rows);
        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->size);
    }

    public function test_a_narrow_first_row_does_not_skew_the_estimate(): void
    {
        $extractor = from_csv(
            path('memory://source.csv'),
            filesystem: CSVFixtureContext::memory(CSVFixtureContext::narrowFirstRow()),
        )->inferSchema(infer_schema()->sampleSize(20));
        $extractor->schema();

        $rows = $extractor->statistics()->rows;

        static::assertEquals(Cardinality::approximately(1050, Cardinality::DEFAULT_RELATIVE_ERROR), $rows);
        static::assertLessThanOrEqual($rows->relativeError, abs(1050 - 1000) / 1000);
    }

    public function test_an_empty_file_declares_zero_rows_exactly(): void
    {
        $extractor = from_csv(CSVFixtureContext::path('empty.csv'));
        $extractor->schema();

        static::assertEquals(Cardinality::exact(0), $extractor->statistics()->rows);
        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->size);
    }

    public function test_extract_samples_like_schema_does(): void
    {
        $extractor = from_csv(CSVFixtureContext::path('five_rows.csv'));

        iterator_to_array($extractor->extract(flow_context()), false);

        static::assertEquals(Cardinality::exact(5), $extractor->statistics()->rows);
    }

    public function test_it_declares_the_listed_byte_total_exactly(): void
    {
        $extractor = from_csv(
            path('memory://source.csv'),
            filesystem: CSVFixtureContext::memory(CSVFixtureContext::tenByteRows(100)),
        );

        static::assertEquals(Cardinality::exact(1008), $extractor->statistics()->size);
    }

    public function test_it_estimates_rows_from_bytes_and_the_mean_sampled_row(): void
    {
        $extractor = from_csv(
            path('memory://source.csv'),
            filesystem: CSVFixtureContext::memory(CSVFixtureContext::tenByteRows(100)),
        )->inferSchema(infer_schema()->sampleSize(10));
        $extractor->schema();

        static::assertEquals(
            Cardinality::approximately(101, Cardinality::DEFAULT_RELATIVE_ERROR),
            $extractor->statistics()->rows,
        );
    }

    public function test_statistics_are_computed_at_most_once(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_csv(CSVFixtureContext::path('five_rows.csv'), filesystem: $filesystem);
        $extractor->schema();
        $listCalls = $filesystem->listCalls;

        static::assertSame($extractor->statistics(), $extractor->statistics());
        static::assertSame($listCalls + 1, $filesystem->listCalls);
    }

    public function test_statistics_before_schema_declares_unknown_rows(): void
    {
        $extractor = from_csv(CSVFixtureContext::path('five_rows.csv'));

        static::assertEquals(Cardinality::unknown(), $extractor->statistics()->rows);

        $extractor->schema();

        static::assertEquals(Cardinality::exact(5), $extractor->statistics()->rows);
    }

    public function test_statistics_before_a_sample_list_the_source_once(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_csv(CSVFixtureContext::path('five_rows.csv'), filesystem: $filesystem);

        $extractor->statistics();
        $extractor->statistics();

        static::assertSame(1, $filesystem->listCalls);
    }
}
