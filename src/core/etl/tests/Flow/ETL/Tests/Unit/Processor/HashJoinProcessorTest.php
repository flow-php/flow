<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Processor\HashJoinProcessor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

final class HashJoinProcessorTest extends FlowTestCase
{
    public function test_handles_empty_left_side(): void
    {
        $rightDf = df()->read(from_rows(rows(row(int_entry('user_id', 1), str_entry('name', 'Alice')))));

        $processor = new HashJoinProcessor($rightDf, Expression::on(['id' => 'user_id']), Join::inner);

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(0, $result);
    }

    public function test_handles_empty_right_side(): void
    {
        $rightDf = df()->read(from_rows(rows()));

        $processor = new HashJoinProcessor($rightDf, Expression::on(['id' => 'user_id']), Join::inner);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1), int_entry('amount', 100)));
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($result as $batch) {
            // @mago-ignore analysis:mixed-assignment
            // @mago-ignore analysis:invalid-iterator,mixed-method-access
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(0, $allRows);
    }

    public function test_inner_join(): void
    {
        $rightDf = df()->read(from_rows(rows(
            row(int_entry('user_id', 1), str_entry('name', 'Alice')),
            row(int_entry('user_id', 2), str_entry('name', 'Bob')),
        )));

        $processor = new HashJoinProcessor($rightDf, Expression::on(['id' => 'user_id']), Join::inner);

        $generator = (static function () {
            yield rows(
                row(int_entry('id', 1), int_entry('amount', 100)),
                row(int_entry('id', 2), int_entry('amount', 200)),
                row(int_entry('id', 3), int_entry('amount', 300)),
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($result as $batch) {
            // @mago-ignore analysis:mixed-assignment
            // @mago-ignore analysis:invalid-iterator,mixed-method-access
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(2, $allRows);
        // @mago-ignore analysis:mixed-array-access
        static::assertEquals(1, $allRows[0]['id']);
        // @mago-ignore analysis:mixed-array-access
        static::assertEquals('Alice', $allRows[0]['name']);
        // @mago-ignore analysis:mixed-array-access
        static::assertEquals(2, $allRows[1]['id']);
        // @mago-ignore analysis:mixed-array-access
        static::assertEquals('Bob', $allRows[1]['name']);
    }

    public function test_left_join(): void
    {
        $rightDf = df()->read(from_rows(rows(row(int_entry('user_id', 1), str_entry('name', 'Alice')))));

        $processor = new HashJoinProcessor($rightDf, Expression::on(['id' => 'user_id']), Join::left);

        $generator = (static function () {
            yield rows(
                row(int_entry('id', 1), int_entry('amount', 100)),
                row(int_entry('id', 2), int_entry('amount', 200)),
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($result as $batch) {
            // @mago-ignore analysis:mixed-assignment
            // @mago-ignore analysis:invalid-iterator,mixed-method-access
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(2, $allRows);
        // @mago-ignore analysis:mixed-array-access
        static::assertEquals('Alice', $allRows[0]['name']);
        // @mago-ignore analysis:mixed-array-access
        static::assertNull($allRows[1]['name']);
    }
}
