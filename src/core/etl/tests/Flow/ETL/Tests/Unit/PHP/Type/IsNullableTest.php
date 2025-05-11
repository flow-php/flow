<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type;

use function Flow\ETL\DSL\{is_nullable, type_null, type_optional, type_string, type_union};
use Flow\ETL\Tests\FlowTestCase;

final class IsNullableTest extends FlowTestCase
{
    public function test_is_nullable() : void
    {
        self::assertTrue(
            is_nullable(type_optional(type_string()))
        );
        self::assertFalse(
            is_nullable(type_string())
        );
        self::assertTrue(
            is_nullable(type_union(type_string(), type_null()))
        );
    }

    public function test_that_union_with_optional_type_is_not_considered_optional() : void
    {
        self::assertFalse(
            is_nullable(type_union(type_string(), type_optional(type_string())))
        );
    }
}
