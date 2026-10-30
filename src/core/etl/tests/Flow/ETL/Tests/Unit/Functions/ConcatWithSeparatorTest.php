<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Functions\ConcatWithSeparator;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\concat_ws;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class ConcatWithSeparatorTest extends FlowTestCase
{
    public function test_concat_with_separator(): void
    {
        static::assertSame('a,1', (new FunctionContext(flow_context()))->eval(
            concat_ws(lit(','), ref('string'), ref('integer')),
            [
                'string' => 'a',
                'integer' => 1,
            ],
            schema(str_schema('string'), int_schema('integer')),
        ));
    }

    public function test_a_null_value_is_skipped(): void
    {
        static::assertSame('a', (new FunctionContext(flow_context()))->eval(
            concat_ws(lit(','), ref('string'), ref('missing_value')),
            [
                'string' => 'a',
                'missing_value' => null,
            ],
            schema(str_schema('string'), str_schema('missing_value', nullable: true)),
        ));
    }

    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();
        $function = new ConcatWithSeparator(lit(','), ref('string'), lit('x'));
        $rebuilt = DescribeRoundTrip::rebuilt($function);
        $rows = ScalarFunctionFixtures::rows(new PhpBackend());
        $schema = ScalarFunctionFixtures::schema();

        static::assertSame($context->exported($function, $rows, $schema), $context->exported($rebuilt, $rows, $schema));
    }
}
