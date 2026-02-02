<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use function Flow\ETL\DSL\{flow_context, int_entry, ref, row, rows, str_entry};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Processor\BatchingByProcessor;
use Flow\ETL\Tests\FlowTestCase;

final class BatchingByProcessorTest extends FlowTestCase
{
    public function test_groups_rows_by_column_value() : void
    {
        $processor = new BatchingByProcessor(ref('group'));

        $generator = (static function () {
            yield rows(
                row(str_entry('group', 'a'), int_entry('id', 1)),
                row(str_entry('group', 'a'), int_entry('id', 2)),
                row(str_entry('group', 'b'), int_entry('id', 3)),
                row(str_entry('group', 'b'), int_entry('id', 4)),
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(2, $result);
        self::assertCount(2, $result[0]);
        self::assertCount(2, $result[1]);

        self::assertEquals('a', $result[0]->first()->valueOf('group'));
        self::assertEquals('b', $result[1]->first()->valueOf('group'));
    }

    public function test_handles_empty_input() : void
    {
        $processor = new BatchingByProcessor(ref('group'));

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(0, $result);
    }

    public function test_handles_single_group() : void
    {
        $processor = new BatchingByProcessor(ref('group'));

        $generator = (static function () {
            yield rows(
                row(str_entry('group', 'a'), int_entry('id', 1)),
                row(str_entry('group', 'a'), int_entry('id', 2)),
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(1, $result);
        self::assertCount(2, $result[0]);
    }

    public function test_respects_min_size() : void
    {
        $processor = new BatchingByProcessor(ref('group'), minSize: 3);

        $generator = (static function () {
            yield rows(
                row(str_entry('group', 'a'), int_entry('id', 1)),
                row(str_entry('group', 'a'), int_entry('id', 2)),
                row(str_entry('group', 'b'), int_entry('id', 3)),
                row(str_entry('group', 'b'), int_entry('id', 4)),
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(1, $result);
        self::assertCount(4, $result[0]);
    }

    public function test_throws_exception_for_invalid_min_size() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Minimum batch size must be greater than 0');

        /** @phpstan-ignore-next-line */
        new BatchingByProcessor(ref('group'), minSize: 0);
    }
}
