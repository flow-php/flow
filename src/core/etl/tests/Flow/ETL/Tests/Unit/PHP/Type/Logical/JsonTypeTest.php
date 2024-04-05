<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\{type_int, type_json};
use PHPUnit\Framework\TestCase;

final class JsonTypeTest extends TestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_json()->isEqual(type_json())
        );
        self::assertFalse(
            type_json()->isEqual(type_int())
        );
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_json(true)->isValid(null));
        self::assertTrue(type_json()->isValid('{"foo": "bar"}'));
        self::assertFalse(type_json()->isValid('{"foo": "bar"'));
        self::assertFalse(type_json()->isValid('2'));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'json',
            type_json()->toString()
        );
        self::assertSame(
            '?json',
            type_json(true)->toString()
        );
    }
}
