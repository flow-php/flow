<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\{type_float, type_null};
use function Flow\ETL\DSL\{type_map, type_string};
use Flow\ETL\Tests\FlowTestCase;

final class NullTypeTest extends FlowTestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_null()->isEqual(type_null())
        );
        self::assertFalse(
            type_null()->isEqual(type_map(type_string(), type_float()))
        );
        self::assertFalse(
            type_null()->isEqual(type_float())
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'null',
            type_null()->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_null()->isValid(null)
        );
        self::assertFalse(
            type_null()->isValid('one')
        );
        self::assertFalse(
            type_null()->isValid([1, 2])
        );
        self::assertFalse(
            type_null()->isValid(123)
        );
    }
}
