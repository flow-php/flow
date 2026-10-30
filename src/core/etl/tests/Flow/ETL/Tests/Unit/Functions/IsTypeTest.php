<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Functions\IsType;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\ref;
use function Flow\Types\DSL\type_integer;

final class IsTypeTest extends FlowTestCase
{
    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();
        $function = new IsType(ref('string'), 'string', type_integer());
        $rebuilt = DescribeRoundTrip::rebuilt($function);
        $rows = ScalarFunctionFixtures::rows(new PhpBackend());
        $schema = ScalarFunctionFixtures::schema();

        static::assertSame($context->exported($function, $rows, $schema), $context->exported($rebuilt, $rows, $schema));
    }
}
