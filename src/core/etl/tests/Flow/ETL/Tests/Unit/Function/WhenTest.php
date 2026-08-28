<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\Literal;
use Flow\ETL\Function\When;
use Flow\ETL\Row;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\when;

final class WhenTest extends FlowTestCase
{
    public function test_a_falsy_else_branch_is_returned(): void
    {
        static::assertFalse(when(ref('active'), lit('yes'), false)->eval(
            row(bool_entry('active', false)),
            flow_context(),
        ));
        static::assertSame(0, when(ref('active'), lit('yes'), 0)->eval(
            row(bool_entry('active', false)),
            flow_context(),
        ));
        static::assertSame('', when(ref('active'), lit('yes'), '')->eval(
            row(bool_entry('active', false)),
            flow_context(),
        ));
        static::assertSame(
            [],
            when(ref('active'), lit('yes'), [])->eval(row(bool_entry('active', false)), flow_context()),
        );
        static::assertNull(when(ref('active'), lit('yes'))->eval(row(bool_entry('active', false)), flow_context()));
    }

    public function test_condition_not_satisfied_without_else(): void
    {
        static::assertSame(1, (new When(ref('id')->equals(lit(2)), new Literal('then'), ref('id')))->eval(
            Row::with(int_entry('id', 1)),
            flow_context(),
        ));
    }

    public function test_else(): void
    {
        static::assertSame('else', (new When(new Literal(false), new Literal('then'), new Literal('else')))->eval(
            Row::with(int_entry('id', 1)),
            flow_context(),
        ));
    }

    public function test_when(): void
    {
        static::assertSame('then', (new When(new Literal(true), new Literal('then')))->eval(
            Row::with(int_entry('id', 1)),
            flow_context(),
        ));
    }
}
