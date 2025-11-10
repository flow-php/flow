<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, int_entry, lit, ref};
use Flow\ETL\Function\{Literal, When};
use Flow\ETL\Row;
use Flow\ETL\Tests\FlowTestCase;

final class WhenTest extends FlowTestCase
{
    public function test_condition_not_satisfied_without_else() : void
    {
        self::assertSame(
            1,
            (new When(
                ref('id')->equals(lit(2)),
                new Literal('then'),
                ref('id')
            ))->eval(Row::with(int_entry('id', 1)), flow_context())
        );
    }

    public function test_else() : void
    {
        self::assertSame(
            'else',
            (new When(
                new Literal(false),
                new Literal('then'),
                new Literal('else'),
            ))->eval(Row::with(int_entry('id', 1)), flow_context())
        );
    }

    public function test_when() : void
    {
        self::assertSame(
            'then',
            (new When(
                new Literal(true),
                new Literal('then')
            ))->eval(Row::with(int_entry('id', 1)), flow_context())
        );
    }
}
