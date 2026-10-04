<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function\Evaluation;

use Flow\ETL\Function\Evaluation\TolerantEvaluation;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\Context\IdRows;
use Flow\ETL\Tests\Double\FailingOnValuesFunction;
use Flow\ETL\Tests\FlowTestCase;

use function array_keys;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function range;

final class TolerantEvaluationTest extends FlowTestCase
{
    public function test_an_empty_batch_has_no_failures_and_no_values(): void
    {
        $rows = array_to_rows([], schema(int_schema('id')));

        static::assertSame(
            ['failed' => [], 'values' => []],
            (new TolerantEvaluation())->evaluate(
                (new ReferenceResolver())->resolve(new FailingOnValuesFunction(ref('id')), $rows->schema()),
                $rows,
                flow_context(),
            ),
        );
    }

    public function test_every_row_evaluates_in_one_batch_when_none_fails(): void
    {
        $spy = new FailingOnValuesFunction(ref('id'));
        $rows = IdRows::batches(range(1, 3))->current();

        static::assertSame(
            ['failed' => [], 'values' => [0 => 1, 1 => 2, 2 => 3]],
            (new TolerantEvaluation())->evaluate(
                (new ReferenceResolver())->resolve($spy, $rows->schema()),
                $rows,
                flow_context(),
            ),
        );
        static::assertSame(1, $spy->evals);
    }

    public function test_failing_rows_are_reported_and_left_out_of_the_values(): void
    {
        $spy = new FailingOnValuesFunction(ref('id'), [2, 4]);
        $rows = IdRows::batches(range(1, 5))->current();

        static::assertEquals(
            ['failed' => [1 => true, 3 => true], 'values' => [0 => 1, 2 => 3, 4 => 5]],
            (new TolerantEvaluation())->evaluate(
                (new ReferenceResolver())->resolve($spy, $rows->schema()),
                $rows,
                flow_context(),
            ),
        );
        static::assertSame(3, $spy->evals);
    }

    public function test_rows_past_the_retry_limit_go_one_by_one(): void
    {
        $spy = new FailingOnValuesFunction(ref('id'), range(1, 9));
        $rows = IdRows::batches(range(1, 10))->current();

        $evaluated = (new TolerantEvaluation())->evaluate(
            (new ReferenceResolver())->resolve($spy, $rows->schema()),
            $rows,
            flow_context(),
        );

        static::assertSame(range(0, 8), array_keys($evaluated['failed']));
        static::assertSame([9 => 10], $evaluated['values']);
        static::assertSame(TolerantEvaluation::RETRIES + 2, $spy->evals);
    }

    public function test_row_by_row_evaluates_each_given_row_alone(): void
    {
        $spy = new FailingOnValuesFunction(ref('id'), [2]);
        $rows = IdRows::batches(range(1, 4))->current();

        static::assertSame(
            ['failed' => [1 => true], 'values' => [0 => 1, 3 => 4]],
            (new TolerantEvaluation())->rowByRow(
                (new ReferenceResolver())->resolve($spy, $rows->schema()),
                $rows,
                [0, 1, 3],
                flow_context(),
            ),
        );
        static::assertSame(3, $spy->evals);
    }
}
