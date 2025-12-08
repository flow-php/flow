<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Schema\Function;

use Flow\PgQuery\Protobuf\AST\FunctionParameterMode;
use Flow\PgQuery\QueryBuilder\Schema\Function\{ArgumentMode, FunctionVolatility, ParallelSafety};
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class EnumsTest extends TestCase
{
    #[Test]
    public function test_argument_mode_in() : void
    {
        self::assertSame(FunctionParameterMode::FUNC_PARAM_IN, ArgumentMode::IN->value);
    }

    #[Test]
    public function test_argument_mode_inout() : void
    {
        self::assertSame(FunctionParameterMode::FUNC_PARAM_INOUT, ArgumentMode::INOUT->value);
    }

    #[Test]
    public function test_argument_mode_out() : void
    {
        self::assertSame(FunctionParameterMode::FUNC_PARAM_OUT, ArgumentMode::OUT->value);
    }

    #[Test]
    public function test_argument_mode_variadic() : void
    {
        self::assertSame(FunctionParameterMode::FUNC_PARAM_VARIADIC, ArgumentMode::VARIADIC->value);
    }

    #[Test]
    public function test_function_volatility_immutable() : void
    {
        self::assertSame('immutable', FunctionVolatility::IMMUTABLE->value);
    }

    #[Test]
    public function test_function_volatility_stable() : void
    {
        self::assertSame('stable', FunctionVolatility::STABLE->value);
    }

    #[Test]
    public function test_function_volatility_volatile() : void
    {
        self::assertSame('volatile', FunctionVolatility::VOLATILE->value);
    }

    #[Test]
    public function test_parallel_safety_restricted() : void
    {
        self::assertSame('restricted', ParallelSafety::RESTRICTED->value);
    }

    #[Test]
    public function test_parallel_safety_safe() : void
    {
        self::assertSame('safe', ParallelSafety::SAFE->value);
    }

    #[Test]
    public function test_parallel_safety_unsafe() : void
    {
        self::assertSame('unsafe', ParallelSafety::UNSAFE->value);
    }
}
