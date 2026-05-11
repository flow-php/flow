<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_is_nullable;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class IsNullableTest extends TestCase
{
    public function test_is_nullable(): void
    {
        static::assertTrue(type_is_nullable(type_optional(type_string())));
        static::assertFalse(type_is_nullable(type_string()));
        static::assertTrue(type_is_nullable(type_union(type_string(), type_null())));
    }

    public function test_that_union_with_optional_type_is_not_considered_optional(): void
    {
        static::assertFalse(type_is_nullable(type_union(type_string(), type_optional(type_string()))));
    }
}
