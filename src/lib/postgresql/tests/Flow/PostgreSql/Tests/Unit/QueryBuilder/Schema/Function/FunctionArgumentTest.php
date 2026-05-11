<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Function;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Schema\Function\ArgumentMode;
use Flow\PostgreSql\QueryBuilder\Schema\Function\FunctionArgument;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class FunctionArgumentTest extends TestCase
{
    #[Test]
    public function test_function_argument_basic(): void
    {
        $arg = FunctionArgument::of(ColumnType::integer());

        static::assertInstanceOf(ColumnType::class, $arg->type);
        static::assertNull($arg->name);
        static::assertSame(ArgumentMode::IN, $arg->mode);
        static::assertNull($arg->default);
    }

    #[Test]
    public function test_function_argument_in_mode(): void
    {
        $arg = FunctionArgument::of(ColumnType::text())->in();

        static::assertSame(ArgumentMode::IN, $arg->mode);
    }

    #[Test]
    public function test_function_argument_inout_mode(): void
    {
        $arg = FunctionArgument::of(ColumnType::text())->inout();

        static::assertSame(ArgumentMode::INOUT, $arg->mode);
    }

    #[Test]
    public function test_function_argument_named(): void
    {
        $arg = FunctionArgument::of(ColumnType::text())->named('username');

        static::assertSame('username', $arg->name);
        static::assertInstanceOf(ColumnType::class, $arg->type);
    }

    #[Test]
    public function test_function_argument_out_mode(): void
    {
        $arg = FunctionArgument::of(ColumnType::integer())->out();

        static::assertSame(ArgumentMode::OUT, $arg->mode);
    }

    #[Test]
    public function test_function_argument_variadic_mode(): void
    {
        $arg = FunctionArgument::of(ColumnType::text())->variadic();

        static::assertSame(ArgumentMode::VARIADIC, $arg->mode);
    }

    #[Test]
    public function test_function_argument_with_default(): void
    {
        $arg = FunctionArgument::of(ColumnType::integer())->default('0');

        static::assertSame('0', $arg->default);
    }

    #[Test]
    public function test_function_argument_with_mode_and_name_and_default(): void
    {
        $arg = FunctionArgument::of(ColumnType::text())->named('value')->out()->default('null');

        static::assertInstanceOf(ColumnType::class, $arg->type);
        static::assertSame('value', $arg->name);
        static::assertSame(ArgumentMode::OUT, $arg->mode);
        static::assertSame('null', $arg->default);
    }
}
