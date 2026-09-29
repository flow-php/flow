<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

final class NotTest extends FlowTestCase
{
    public function test_not_over_a_null_predicate_drops_the_row(): void
    {
        $kept = df()
            ->read(from_array([
                ['id' => 1, 'score' => 50],
                ['id' => 2, 'score' => null],
                ['id' => 3, 'score' => 5],
            ]))
            ->filter(ref('score')->greaterThan(lit(10)))
            ->fetch()
            ->toArray();

        static::assertSame([['id' => 1, 'score' => 50]], $kept);

        $keptNot = df()
            ->read(from_array([
                ['id' => 1, 'score' => 50],
                ['id' => 2, 'score' => null],
                ['id' => 3, 'score' => 5],
            ]))
            ->filter(not(ref('score')->greaterThan(lit(10))))
            ->fetch()
            ->toArray();

        // SQL: NOT NULL is NULL, so the null row drops on both sides of the predicate.
        static::assertSame([['id' => 3, 'score' => 5]], $keptNot);
    }

    public function test_not_expression_on_array_true_value(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(not(lit([1, 2, 3])), [], schema()));
    }

    public function test_not_expression_on_boolean_true_value(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(not(lit(true)), [], schema()));
    }

    public function test_not_expression_on_is_in_expression(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            not(ref('value')->isIn(ref('array'))),
            ['array' => [1, 2, 3], 'value' => 10],
            schema(list_schema('array', type_list(type_integer())), int_schema('value')),
        ));
    }

    public function test_not_expression_with_and_operator(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            not(ref('value')->isNull()->or(ref('value')->isType(type_integer()))),
            ['value' => '10'],
            schema(str_schema('value')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            not(ref('value')->isNull()->or(ref('value')->isType(type_integer()))),
            ['value' => null],
            schema(str_schema('value', nullable: true)),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            not(ref('value')->isNull()->and(ref('value')->size()->between(1, 10))),
            ['value' => 'abcd'],
            schema(str_schema('value')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            not(ref('value')->isNull()->or(ref('value')->size()->equals(1))),
            ['value' => 'abcd'],
            schema(str_schema('value')),
        ));
    }
}
