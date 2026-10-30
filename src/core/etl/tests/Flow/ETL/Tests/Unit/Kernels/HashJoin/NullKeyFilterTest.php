<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\HashJoin;

use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Kernels\HashJoin\NullKeyFilter;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\Context\RowsStorageContext;
use Flow\ETL\Tests\Double\StopRecordingBatches;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class NullKeyFilterTest extends FlowTestCase
{
    public function test_drops_rows_with_a_null_in_any_key_column(): void
    {
        static::assertSame(
            [['k' => 1, 'l' => 1]],
            (new NullKeyFilter(['k', 'l']))->of(array_to_rows(
                [['k' => 1, 'l' => 1], ['k' => null, 'l' => 1], ['k' => 2, 'l' => null]],
                schema(int_schema('k', true), int_schema('l', true)),
            ))->toArray(),
        );
    }

    public function test_a_batch_without_nulls_is_returned_as_it_is(): void
    {
        $batch = array_to_rows([['k' => 1], ['k' => 2]], schema(int_schema('k', true)));

        static::assertSame($batch, (new NullKeyFilter(['k']))->of($batch));
    }

    public function test_rows_drops_the_rows_and_the_emptied_batches(): void
    {
        static::assertSame(
            [['k' => 1], ['k' => 2], ['k' => 3]],
            RowsStorageContext::rows((new NullKeyFilter(['k']))->rows(GroupByContext::batches(
                array_to_rows([['k' => 1], ['k' => null], ['k' => 2]], schema(int_schema('k', true))),
                array_to_rows([['k' => null]], schema(int_schema('k', true))),
                array_to_rows([['k' => 3]], schema(int_schema('k', true))),
            ))),
        );
    }

    public function test_rows_forwards_a_sent_value_and_returns(): void
    {
        $batches = new StopRecordingBatches(
            array_to_rows([['k' => 1]], schema(int_schema('k', true))),
            array_to_rows([['k' => 2]], schema(int_schema('k', true))),
        );
        $rows = (new NullKeyFilter(['k']))->rows($batches->rows());
        $rows->current();
        $rows->send(Signal::STOP);

        static::assertFalse($rows->valid());
        static::assertTrue($batches->stopped);
        static::assertSame(1, $batches->read);
    }
}
