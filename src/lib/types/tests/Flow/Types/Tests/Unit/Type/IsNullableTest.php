<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use function Flow\Types\DSL\{type_is_nullable, type_null, type_optional, type_string, type_union};
use PHPUnit\Framework\TestCase;

final class IsNullableTest extends TestCase
{
    public function test_is_nullable() : void
    {
        self::assertTrue(
            type_is_nullable(type_optional(type_string()))
        );
        self::assertFalse(
            type_is_nullable(type_string())
        );
        self::assertTrue(
            type_is_nullable(type_union(type_string(), type_null()))
        );
    }

    public function test_that_union_with_optional_type_is_not_considered_optional() : void
    {
        self::assertFalse(
            type_is_nullable(type_union(type_string(), type_optional(type_string())))
        );
    }
}
