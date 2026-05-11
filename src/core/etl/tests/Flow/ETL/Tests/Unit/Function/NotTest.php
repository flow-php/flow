<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\string_entry;
use function Flow\Types\DSL\type_integer;

final class NotTest extends FlowTestCase
{
    public function test_not_expression_on_array_true_value(): void
    {
        static::assertFalse(not(lit([1, 2, 3]))->eval(row(), flow_context()));
    }

    public function test_not_expression_on_boolean_true_value(): void
    {
        static::assertFalse(not(lit(true))->eval(row(), flow_context()));
    }

    public function test_not_expression_on_is_in_expression(): void
    {
        static::assertTrue(
            not(ref('value')->isIn(ref('array')))
                ->eval(row(json_entry('array', [1, 2, 3]), int_entry('value', 10)), flow_context()),
        );
    }

    public function test_not_expression_with_and_operator(): void
    {
        static::assertTrue(
            not(ref('value')->isNull()->or(ref('value')->isType(type_integer())))
                ->eval(row(string_entry('value', '10')), flow_context()),
        );
        static::assertFalse(
            not(ref('value')->isNull()->or(ref('value')->isType(type_integer())))
                ->eval(row(string_entry('value', null)), flow_context()),
        );
        static::assertTrue(
            not(ref('value')->isNull()->and(ref('value')->size()->between(1, 10)))
                ->eval(row(string_entry('value', 'abcd')), flow_context()),
        );
        static::assertTrue(
            not(ref('value')->isNull()->or(ref('value')->size()->equals(1)))
                ->eval(row(string_entry('value', 'abcd')), flow_context()),
        );
    }
}
