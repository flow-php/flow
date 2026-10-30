<?php

declare(strict_types=1);

namespace Flow\ETL\Tests;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Generator;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function getenv;
use function max;

/**
 * Base test case for testing FLow, designed mostly for unit tests.
 * In case of integration tests, use FlowIntegrationTestCase that extends this class.
 */
abstract class FlowTestCase extends TestCase
{
    /**
     * $factory returns a FRESH extractor per call, because every check below drains its own.
     *
     * @param callable(): (BatchableExtractor&Extractor) $factory
     */
    final public static function assertExtractorHonoursBatchContract(
        callable $factory,
        Rows $expected,
        string $message = '',
    ): void {
        foreach ([1, 3, 1000] as $size) {
            $collected = rows(schema());
            $extractor = $factory()->withBatchSize($size);

            foreach ($extractor->extract(flow_context()->backend(), new Criteria()) as $batch) {
                static::assertLessThanOrEqual($extractor->batchSize(), $batch->count(), $message);
                $collected = $collected->isEmpty()
                    ? $batch
                    : Rows::concat($collected->schema(), new PhpBackend(), $collected, $batch);
            }

            static::assertEquals($expected->toArray(), $collected->toArray(), $message);
        }

        // the assertion an unbatched extractor fails
        if ($expected->count() > 1) {
            $largest = 0;

            foreach ($factory()->withBatchSize(1000)->extract(flow_context()->backend(), new Criteria()) as $batch) {
                $largest = max($largest, $batch->count());
            }

            static::assertGreaterThan(1, $largest, $message);
        }

        // size 1, so any fixture of two rows or more yields a second batch that STOP must prevent
        $generator = $factory()->withBatchSize(1)->extract(flow_context()->backend(), new Criteria());
        static::assertTrue($generator->valid(), $message);
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid(), $message);

        $extractor = $factory();

        if ($extractor->isRepeatable()) {
            $first = rows(schema());
            $second = rows(schema());

            foreach ($extractor->extract(flow_context()->backend(), new Criteria()) as $batch) {
                $first = $first->isEmpty() ? $batch : Rows::concat($first->schema(), new PhpBackend(), $first, $batch);
            }

            foreach ($extractor->extract(flow_context()->backend(), new Criteria()) as $batch) {
                $second = $second->isEmpty()
                    ? $batch
                    : Rows::concat($second->schema(), new PhpBackend(), $second, $batch);
            }

            static::assertEquals($first->toArray(), $second->toArray(), $message);
        }
    }

    /**
     * withMaximum() is source configuration, not a plan operator, so nothing downstream can enforce it.
     *
     * @param callable(int): (BatchableExtractor&Extractor) $factory
     */
    final public static function assertExtractorHonoursMaximum(
        callable $factory,
        int $maximum,
        string $message = '',
    ): void {
        $extractor = $factory($maximum)->withBatchSize(1000);
        $total = 0;

        foreach ($extractor->extract(flow_context()->backend(), new Criteria()) as $batch) {
            static::assertLessThanOrEqual(1000, $batch->count(), $message);
            $total += $batch->count();
        }

        static::assertSame($maximum, $total, $message);
    }

    /**
     * @param array<array-key, mixed> $expectedArray
     */
    final public static function assertExtractedRowsAsArrayEquals(
        array $expectedArray,
        Extractor $extractor,
        ?FlowContext $flowContext = null,
        string $message = '',
        Criteria $criteria = new Criteria(),
    ): void {
        $flowContext ??= flow_context();
        $extractedRows = rows(schema());

        foreach (self::extracted($extractor, $flowContext, $criteria) as $nextRows) {
            $extractedRows = $extractedRows->isEmpty()
                ? $nextRows
                : Rows::concat($extractedRows->schema(), new PhpBackend(), $extractedRows, $nextRows);
        }

        static::assertEquals($expectedArray, $extractedRows->toArray(), $message);
    }

    final public static function assertExtractedRowsCount(
        int $expectedCount,
        Extractor $extractor,
        ?FlowContext $flowContext = null,
        string $message = '',
        Criteria $criteria = new Criteria(),
    ): void {
        $flowContext ??= flow_context();
        $totalRows = 0;

        foreach (self::extracted($extractor, $flowContext, $criteria) as $rows) {
            $totalRows += $rows->count();
        }

        static::assertSame($expectedCount, $totalRows, $message);
    }

    final public static function assertExtractedRowsEquals(
        Rows $expectedRows,
        Extractor $extractor,
        ?FlowContext $flowContext = null,
        string $message = '',
        Criteria $criteria = new Criteria(),
    ): void {
        $flowContext ??= flow_context();
        $extractedRows = rows(schema());

        foreach (self::extracted($extractor, $flowContext, $criteria) as $nextRows) {
            $extractedRows = $extractedRows->isEmpty()
                ? $nextRows
                : Rows::concat($extractedRows->schema(), new PhpBackend(), $extractedRows, $nextRows);
        }

        static::assertEquals($expectedRows, $extractedRows, $message);
    }

    /**
     * @return Generator<int, Rows>
     */
    final public static function extracted(Extractor $extractor, FlowContext $context, Criteria $criteria): Generator
    {
        return $extractor->extract($context->backend(), $criteria);
    }

    public function repositoryRoot(): string
    {
        $root = getenv('FLOW_MONOREPO_PROJECT_ROOT');

        if ($root === false || $root === '') {
            throw new RuntimeException('FLOW_MONOREPO_PROJECT_ROOT is not set; bootstrap.php must be loaded.');
        }

        return $root;
    }
}
