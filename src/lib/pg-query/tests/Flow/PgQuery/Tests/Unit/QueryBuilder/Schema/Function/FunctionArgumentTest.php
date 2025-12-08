<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Schema\Function;

use Flow\PgQuery\QueryBuilder\Schema\Function\{ArgumentMode, FunctionArgument};
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class FunctionArgumentTest extends TestCase
{
    #[Test]
    public function test_function_argument_basic() : void
    {
        $arg = FunctionArgument::of('integer');

        self::assertSame('integer', $arg->type);
        self::assertNull($arg->name);
        self::assertSame(ArgumentMode::IN, $arg->mode);
        self::assertNull($arg->default);
    }

    #[Test]
    public function test_function_argument_in_mode() : void
    {
        $arg = FunctionArgument::of('text')->in();

        self::assertSame(ArgumentMode::IN, $arg->mode);
    }

    #[Test]
    public function test_function_argument_inout_mode() : void
    {
        $arg = FunctionArgument::of('text')->inout();

        self::assertSame(ArgumentMode::INOUT, $arg->mode);
    }

    #[Test]
    public function test_function_argument_named() : void
    {
        $arg = FunctionArgument::of('text')->named('username');

        self::assertSame('username', $arg->name);
        self::assertSame('text', $arg->type);
    }

    #[Test]
    public function test_function_argument_out_mode() : void
    {
        $arg = FunctionArgument::of('integer')->out();

        self::assertSame(ArgumentMode::OUT, $arg->mode);
    }

    #[Test]
    public function test_function_argument_variadic_mode() : void
    {
        $arg = FunctionArgument::of('text')->variadic();

        self::assertSame(ArgumentMode::VARIADIC, $arg->mode);
    }

    #[Test]
    public function test_function_argument_with_default() : void
    {
        $arg = FunctionArgument::of('integer')->default('0');

        self::assertSame('0', $arg->default);
    }

    #[Test]
    public function test_function_argument_with_mode_and_name_and_default() : void
    {
        $arg = FunctionArgument::of('text')
            ->named('value')
            ->out()
            ->default('null');

        self::assertSame('text', $arg->type);
        self::assertSame('value', $arg->name);
        self::assertSame(ArgumentMode::OUT, $arg->mode);
        self::assertSame('null', $arg->default);
    }
}
