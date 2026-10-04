<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Function\Literal;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\When;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Double\FailingOnValuesFunction;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\when;

final class WhenTest extends FlowTestCase
{
    public function test_a_falsy_else_branch_is_returned(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            when(ref('active'), lit(true), false),
            [
                'active' => false,
            ],
            schema(bool_schema('active')),
        ));
        static::assertSame(0, (new FunctionContext(flow_context()))->eval(
            when(ref('active'), lit(1), 0),
            [
                'active' => false,
            ],
            schema(bool_schema('active')),
        ));
        static::assertSame('', (new FunctionContext(flow_context()))->eval(
            when(ref('active'), lit('yes'), ''),
            [
                'active' => false,
            ],
            schema(bool_schema('active')),
        ));
        static::assertSame(
            [],
            (new FunctionContext(flow_context()))->eval(
                when(ref('active'), lit(['yes']), []),
                [
                    'active' => false,
                ],
                schema(bool_schema('active')),
            ),
        );
        static::assertNull((new FunctionContext(flow_context()))->eval(
            when(ref('active'), lit('yes')),
            [
                'active' => false,
            ],
            schema(bool_schema('active')),
        ));
    }

    public function test_condition_not_satisfied_without_else(): void
    {
        // when() of string and integer branches declares string: the value is cast to it
        static::assertSame('1', (new FunctionContext(flow_context()))->eval(
            new When(ref('id')->equals(lit(2)), new Literal('then'), ref('id')),
            [
                'id' => 1,
            ],
            schema(int_schema('id')),
        ));
    }

    public function test_else(): void
    {
        static::assertSame('else', (new FunctionContext(flow_context()))->eval(
            new When(new Literal(false), new Literal('then'), new Literal('else')),
            [
                'id' => 1,
            ],
            schema(int_schema('id')),
        ));
    }

    public function test_when(): void
    {
        static::assertSame('then', (new FunctionContext(flow_context()))->eval(
            new When(new Literal(true), new Literal('then')),
            [
                'id' => 1,
            ],
            schema(int_schema('id')),
        ));
    }

    public function test_then_branch_error_names_the_batch_row(): void
    {
        $rows = array_to_rows(
            [
                ['c' => true, 'v' => 1],
                ['c' => false, 'v' => 99],
                ['c' => true, 'v' => 3],
                ['c' => false, 'v' => 99],
                ['c' => false, 'v' => 99],
                ['c' => true, 'v' => 50],
            ],
            schema(bool_schema('c'), int_schema('v')),
        );

        try {
            (new ReferenceResolver())
                ->resolve(when(ref('c'), new FailingOnValuesFunction(ref('v'), [50]), lit(0)), $rows->schema())
                ->eval($rows, flow_context());
            static::fail('expected an EvaluationException');
        } catch (EvaluationException $e) {
            static::assertSame(5, $e->rowIndex);
        }
    }

    public function test_else_branch_error_names_the_batch_row(): void
    {
        $rows = array_to_rows(
            [
                ['c' => false, 'v' => 1],
                ['c' => true, 'v' => 99],
                ['c' => true, 'v' => 99],
                ['c' => false, 'v' => 50],
                ['c' => true, 'v' => 99],
            ],
            schema(bool_schema('c'), int_schema('v')),
        );

        try {
            (new ReferenceResolver())
                ->resolve(when(ref('c'), lit(0), new FailingOnValuesFunction(ref('v'), [50])), $rows->schema())
                ->eval($rows, flow_context());
            static::fail('expected an EvaluationException');
        } catch (EvaluationException $e) {
            static::assertSame(3, $e->rowIndex);
        }
    }

    public function test_a_branch_runs_only_on_the_rows_that_reach_it(): void
    {
        $rows = array_to_rows(
            [
                ['c' => true, 'v' => 1],
                ['c' => false, 'v' => 99],
                ['c' => true, 'v' => 3],
                ['c' => false, 'v' => 99],
                ['c' => false, 'v' => 99],
                ['c' => true, 'v' => 50],
            ],
            schema(bool_schema('c'), int_schema('v')),
        );

        static::assertSame(
            [1, 0, 3, 0, 0, 50],
            (new ReferenceResolver())
                ->resolve(when(ref('c'), new FailingOnValuesFunction(ref('v'), [99]), lit(0)), $rows->schema())
                ->eval($rows, flow_context())
                ->values(),
        );
    }
}
