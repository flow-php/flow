<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Double\FailingOnValuesFunction;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\coalesce;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CoalesceTest extends FlowTestCase
{
    public function test_a_non_nullable_branch_makes_the_result_not_nullable(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            coalesce(ref('name'), lit('N/A')),
            schema(str_schema('name', nullable: true)),
        );

        static::assertSame('string', $resolved->returns()->toString());
    }

    public function test_coalesce_entries(): void
    {
        // coalesce() of string and integer declares string: the value is cast to it
        static::assertSame('1', (new FunctionContext(flow_context()))->eval(
            coalesce(ref('name'), ref('id'), lit('N/A')),
            [
                'name' => null,
                'id' => 1,
            ],
            schema(str_schema('name', nullable: true), int_schema('id')),
        ));
    }

    public function test_coalesce_on_null_entries_falls_through_to_lit(): void
    {
        static::assertSame('N/A', (new FunctionContext(flow_context()))->eval(
            coalesce(ref('name'), ref('string'), lit('N/A')),
            [
                'name' => null,
                'string' => null,
            ],
            schema(str_schema('name', nullable: true), str_schema('string', nullable: true)),
        ));
    }

    public function test_coalesce_on_ref(): void
    {
        // coalesce() of string and integer declares string: the value is cast to it
        static::assertSame('1', (new FunctionContext(flow_context()))->eval(
            ref('name')->coalesce(ref('id'), lit('N/A')),
            ['name' => null, 'id' => 1],
            schema(str_schema('name', nullable: true), int_schema('id')),
        ));
    }

    public function test_a_throwing_branch_is_not_swallowed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new FunctionContext(flow_context()))->eval(
            coalesce(ref('a')->upper(), ref('b')->upper()),
            ['a' => 1, 'b' => 2],
            schema(int_schema('a'), int_schema('b')),
        );
    }

    public function test_an_unreached_argument_that_would_throw_is_not_evaluated(): void
    {
        $rows = array_to_rows(
            [['a' => 1, 'v' => 99], ['a' => null, 'v' => 2]],
            schema(int_schema('a', nullable: true), int_schema('v')),
        );

        static::assertSame(
            [1, 2],
            (new ReferenceResolver())
                ->resolve(coalesce(ref('a'), new FailingOnValuesFunction(ref('v'), [99])), $rows->schema())
                ->eval($rows, flow_context())
                ->values(),
        );
    }
}
