<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class BatchingProcessorTest extends FlowTestCase
{
    public function test_handles_empty_input(): void
    {
        $processor = new BatchingProcessor(2);
        $generator = (static function () {
            yield from [];
        })();
        $result = iterator_to_array($processor->process($generator, flow_context()));
        static::assertCount(0, $result);
    }

    public function test_handles_exact_batch_size_multiple(): void
    {
        $processor = new BatchingProcessor(2);
        $generator = (static function () {
            yield rows(
                schema(int_schema('id')),
                row(['id' => 1]),
                row(['id' => 2]),
                row(['id' => 3]),
                row(['id' => 4]),
            );
        })();
        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        static::assertCount(2, $result);
        static::assertCount(2, $result[0]);
        static::assertCount(2, $result[1]);
    }

    public function test_handles_single_large_batch(): void
    {
        $processor = new BatchingProcessor(3);
        $generator = (static function () {
            yield rows(
                schema(int_schema('id')),
                row(['id' => 1]),
                row(['id' => 2]),
                row(['id' => 3]),
                row(['id' => 4]),
                row(['id' => 5]),
            );
        })();
        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        static::assertCount(2, $result);
        static::assertCount(3, $result[0]);
        static::assertCount(2, $result[1]);
    }

    public function test_rebatches_rows_into_fixed_size(): void
    {
        $processor = new BatchingProcessor(2);
        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]));
            yield rows(schema(int_schema('id')), row(['id' => 2]));
            yield rows(schema(int_schema('id')), row(['id' => 3]));
            yield rows(schema(int_schema('id')), row(['id' => 4]));
            yield rows(schema(int_schema('id')), row(['id' => 5]));
        })();
        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        static::assertCount(3, $result);
        static::assertCount(2, $result[0]);
        static::assertCount(2, $result[1]);
        static::assertCount(1, $result[2]);
    }

    public function test_throws_exception_for_negative_size(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0');
        // @mago-ignore analysis:invalid-argument
        new BatchingProcessor(-1);
    }

    public function test_throws_exception_for_zero_size(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0');
        // @mago-ignore analysis:invalid-argument
        new BatchingProcessor(0);
    }
}
