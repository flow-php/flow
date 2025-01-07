<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\{type_callable, type_float};
use function Flow\ETL\DSL\{type_map, type_string};
use Flow\ETL\Tests\FlowTestCase;

final class CallableTypeTest extends FlowTestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_callable(false)->isEqual(type_callable(false))
        );
        self::assertFalse(
            type_callable(false)->isEqual(type_map(type_string(), type_float()))
        );
        self::assertFalse(
            type_callable(false)->isEqual(type_float())
        );
        self::assertFalse(
            type_callable(false)->isEqual(type_callable(true))
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'callable',
            type_callable(false)->toString()
        );
        self::assertSame(
            '?callable',
            type_callable(true)->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_callable(false)->isValid('printf')
        );
        self::assertTrue(
            type_callable(true)->isValid(null)
        );
        self::assertFalse(
            type_callable(false)->isValid('one')
        );
        self::assertFalse(
            type_callable(false)->isValid([1, 2])
        );
        self::assertFalse(
            type_callable(false)->isValid(123)
        );
    }
}
