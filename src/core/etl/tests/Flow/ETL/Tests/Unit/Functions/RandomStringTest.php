<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Functions\RandomString;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\lit;

final class RandomStringTest extends FlowTestCase
{
    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();
        $function = new RandomString(lit(5));
        $rebuilt = DescribeRoundTrip::rebuilt($function);
        $rows = ScalarFunctionFixtures::rows(new PhpBackend());
        $schema = ScalarFunctionFixtures::schema();

        static::assertSame(
            $context->columnOf($function, $rows, $schema)->type()->toString(),
            $context->columnOf($rebuilt, $rows, $schema)->type()->toString(),
        );
    }
}
