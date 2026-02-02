<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use function Flow\ETL\DSL\{flow_context, int_entry, row, rows};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Tests\FlowTestCase;

final class BatchingProcessorTest extends FlowTestCase
{
    public function test_handles_empty_input() : void
    {
        $processor = new BatchingProcessor(2);

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(0, $result);
    }

    public function test_handles_exact_batch_size_multiple() : void
    {
        $processor = new BatchingProcessor(2);

        $generator = (static function () {
            yield rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3)),
                row(int_entry('id', 4))
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(2, $result);
        self::assertCount(2, $result[0]);
        self::assertCount(2, $result[1]);
    }

    public function test_handles_single_large_batch() : void
    {
        $processor = new BatchingProcessor(3);

        $generator = (static function () {
            yield rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3)),
                row(int_entry('id', 4)),
                row(int_entry('id', 5))
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(2, $result);
        self::assertCount(3, $result[0]);
        self::assertCount(2, $result[1]);
    }

    public function test_rebatches_rows_into_fixed_size() : void
    {
        $processor = new BatchingProcessor(2);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)));
            yield rows(row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)));
            yield rows(row(int_entry('id', 4)));
            yield rows(row(int_entry('id', 5)));
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(3, $result);
        self::assertCount(2, $result[0]);
        self::assertCount(2, $result[1]);
        self::assertCount(1, $result[2]);
    }

    public function test_throws_exception_for_negative_size() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0');

        /** @phpstan-ignore-next-line */
        new BatchingProcessor(-1);
    }

    public function test_throws_exception_for_zero_size() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0');

        /** @phpstan-ignore-next-line */
        new BatchingProcessor(0);
    }
}
