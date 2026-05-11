<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\DSL;

use Flow\PostgreSql\QueryBuilder\Expression\Parameter;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\parameters;

final class ParametersTest extends TestCase
{
    public function test_parameters_generates_sequential_params(): void
    {
        $params = parameters(3);

        static::assertCount(3, $params);
        static::assertSame(1, $params[0]->number());
        static::assertSame(2, $params[1]->number());
        static::assertSame(3, $params[2]->number());
    }

    public function test_parameters_negative_count_throws(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        parameters(-1);
    }

    public function test_parameters_single(): void
    {
        $params = parameters(1);

        static::assertCount(1, $params);
        static::assertInstanceOf(Parameter::class, $params[0]);
        static::assertSame(1, $params[0]->number());
    }

    public function test_parameters_with_start_at(): void
    {
        $params = parameters(3, startAt: 4);

        static::assertCount(3, $params);
        static::assertSame(4, $params[0]->number());
        static::assertSame(5, $params[1]->number());
        static::assertSame(6, $params[2]->number());
    }

    public function test_parameters_zero_count_throws(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        parameters(0);
    }

    public function test_parameters_zero_start_throws(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        parameters(3, startAt: 0);
    }
}
