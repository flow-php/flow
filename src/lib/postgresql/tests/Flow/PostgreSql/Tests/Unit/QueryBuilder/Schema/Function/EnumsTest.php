<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\FunctionParameterMode;
use Flow\PostgreSql\QueryBuilder\Schema\Function\ArgumentMode;
use Flow\PostgreSql\QueryBuilder\Schema\Function\FunctionVolatility;
use Flow\PostgreSql\QueryBuilder\Schema\Function\ParallelSafety;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class EnumsTest extends TestCase
{
    #[Test]
    public function test_argument_mode_in(): void
    {
        static::assertSame(FunctionParameterMode::FUNC_PARAM_IN, ArgumentMode::IN->value);
    }

    #[Test]
    public function test_argument_mode_inout(): void
    {
        static::assertSame(FunctionParameterMode::FUNC_PARAM_INOUT, ArgumentMode::INOUT->value);
    }

    #[Test]
    public function test_argument_mode_out(): void
    {
        static::assertSame(FunctionParameterMode::FUNC_PARAM_OUT, ArgumentMode::OUT->value);
    }

    #[Test]
    public function test_argument_mode_variadic(): void
    {
        static::assertSame(FunctionParameterMode::FUNC_PARAM_VARIADIC, ArgumentMode::VARIADIC->value);
    }

    #[Test]
    public function test_function_volatility_immutable(): void
    {
        static::assertSame('immutable', FunctionVolatility::IMMUTABLE->value);
    }

    #[Test]
    public function test_function_volatility_stable(): void
    {
        static::assertSame('stable', FunctionVolatility::STABLE->value);
    }

    #[Test]
    public function test_function_volatility_volatile(): void
    {
        static::assertSame('volatile', FunctionVolatility::VOLATILE->value);
    }

    #[Test]
    public function test_parallel_safety_restricted(): void
    {
        static::assertSame('restricted', ParallelSafety::RESTRICTED->value);
    }

    #[Test]
    public function test_parallel_safety_safe(): void
    {
        static::assertSame('safe', ParallelSafety::SAFE->value);
    }

    #[Test]
    public function test_parallel_safety_unsafe(): void
    {
        static::assertSame('unsafe', ParallelSafety::UNSAFE->value);
    }
}
