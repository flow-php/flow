<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\Literal;
use Flow\ETL\Function\When;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
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
        static::assertFalse(when(ref('active'), lit('yes'), false)->eval(array_to_row([
            'active' => false,
        ], schema(bool_schema('active'))), flow_context()));
        static::assertSame(0, when(ref('active'), lit('yes'), 0)->eval(array_to_row([
            'active' => false,
        ], schema(bool_schema('active'))), flow_context()));
        static::assertSame('', when(ref('active'), lit('yes'), '')->eval(array_to_row([
            'active' => false,
        ], schema(bool_schema('active'))), flow_context()));
        static::assertSame(
            [],
            when(ref('active'), lit('yes'), [])->eval(array_to_row([
                'active' => false,
            ], schema(bool_schema('active'))), flow_context()),
        );
        static::assertNull(when(ref('active'), lit('yes'))->eval(array_to_row([
            'active' => false,
        ], schema(bool_schema('active'))), flow_context()));
    }

    public function test_condition_not_satisfied_without_else(): void
    {
        static::assertSame(1, (new When(ref('id')->equals(lit(2)), new Literal('then'), ref('id')))->eval(array_to_row([
            'id' => 1,
        ], schema(int_schema('id'))), flow_context()));
    }

    public function test_else(): void
    {
        static::assertSame('else', (new When(new Literal(false), new Literal('then'), new Literal('else')))->eval(
            array_to_row([
                'id' => 1,
            ], schema(int_schema('id'))),
            flow_context(),
        ));
    }

    public function test_when(): void
    {
        static::assertSame('then', (new When(new Literal(true), new Literal('then')))->eval(array_to_row([
            'id' => 1,
        ], schema(int_schema('id'))), flow_context()));
    }
}
