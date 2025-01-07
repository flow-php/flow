<?php

declare(strict_types=1);

namespace Flow\ETL\Tests;

use function Flow\ETL\DSL\{flow_context, rows};
use Flow\ETL\{Extractor, FlowContext, Rows};
use PHPUnit\Framework\TestCase;

/**
 * Base test case for testing FLow, designed mostly for unit tests.
 * In case of integration tests, use FlowIntegrationTestCase that extends this class.
 */
abstract class FlowTestCase extends TestCase
{
    final public static function assertExtractedBatchesCount(
        int $expectedCount,
        Extractor $extractor,
        ?FlowContext $flowContext = null,
        string $message = '',
    ) : void {
        $flowContext ??= flow_context();

        static::assertCount(
            $expectedCount,
            \iterator_to_array($extractor->extract($flowContext)),
            $message
        );
    }

    final public static function assertExtractedBatchesSize(
        int $expectedCount,
        Extractor $extractor,
        ?FlowContext $flowContext = null,
        string $message = '',
    ) : void {
        $flowContext ??= flow_context();
        $extractorContainsBatches = false;

        foreach ($extractor->extract($flowContext) as $rows) {
            static::assertCount($expectedCount, $rows, $message);
            $extractorContainsBatches = true;
        }

        if (!$extractorContainsBatches) {
            static::fail('Extractor does not contain any batches');
        }
    }

    final public static function assertExtractedRowsAsArrayEquals(
        array $expectedArray,
        Extractor $extractor,
        ?FlowContext $flowContext = null,
        string $message = '',
    ) : void {
        $flowContext ??= flow_context();
        $extractedRows = rows();

        foreach ($extractor->extract($flowContext) as $nextRows) {
            $extractedRows = $extractedRows->merge($nextRows);
        }

        static::assertEquals($expectedArray, $extractedRows->toArray(), $message);
    }

    final public static function assertExtractedRowsCount(
        int $expectedCount,
        Extractor $extractor,
        ?FlowContext $flowContext = null,
        string $message = '',
    ) : void {
        $flowContext ??= flow_context();
        $totalRows = 0;

        foreach ($extractor->extract($flowContext) as $rows) {
            $totalRows += $rows->count();
        }

        static::assertSame($expectedCount, $totalRows, $message);
    }

    final public static function assertExtractedRowsEquals(
        Rows $expectedRows,
        Extractor $extractor,
        ?FlowContext $flowContext = null,
        string $message = '',
    ) : void {
        $flowContext ??= flow_context();
        $extractedRows = rows();

        foreach ($extractor->extract($flowContext) as $nextRows) {
            $extractedRows = $extractedRows->merge($nextRows);
        }

        static::assertEquals($expectedRows, $extractedRows, $message);
    }
}
