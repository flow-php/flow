<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_json;
use PHPUnit\Framework\TestCase;

final class JsonTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            type_json()->isEqual(type_json())
        );
        $this->assertFalse(
            type_json()->isEqual(type_int())
        );
    }

    public function test_is_valid() : void
    {
        $this->assertTrue(type_json()->isValid('{"foo": "bar"}'));
        $this->assertFalse(type_json()->isValid('{"foo": "bar"'));
        $this->assertFalse(type_json()->isValid('2'));
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'json',
            type_json()->toString()
        );
        $this->assertSame(
            '?json',
            type_json(true)->toString()
        );
    }
}
