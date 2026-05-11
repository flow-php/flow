<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use Flow\PostgreSql\Client\Telemetry\QueryAttributes;
use PHPUnit\Framework\TestCase;

final class QueryAttributesTest extends TestCase
{
    public function test_create_with_both_values(): void
    {
        $attrs = new QueryAttributes('SELECT', 'users');

        static::assertSame('SELECT', $attrs->operation);
        static::assertSame('users', $attrs->target);
    }

    public function test_create_with_null_operation(): void
    {
        $attrs = new QueryAttributes(null, 'users');

        static::assertNull($attrs->operation);
        static::assertSame('users', $attrs->target);
    }

    public function test_create_with_null_target(): void
    {
        $attrs = new QueryAttributes('SELECT', null);

        static::assertSame('SELECT', $attrs->operation);
        static::assertNull($attrs->target);
    }

    public function test_create_with_null_values(): void
    {
        $attrs = new QueryAttributes(null, null);

        static::assertNull($attrs->operation);
        static::assertNull($attrs->target);
    }
}
