<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\type_array;
use Flow\ETL\PHP\Type\Caster\ArrayCastingHandler;
use PHPUnit\Framework\TestCase;

final class ArrayCastingHandlerTest extends TestCase
{
    public function test_casting_boolean_to_array() : void
    {
        $this->assertEquals(
            [true],
            (new ArrayCastingHandler())->value(true, type_array())
        );
    }

    public function test_casting_datetime_to_array() : void
    {
        $this->assertEquals(
            ['date' => '2021-01-01 00:00:00.000000', 'timezone_type' => 3, 'timezone' => 'UTC'],
            (new ArrayCastingHandler())->value(new \DateTimeImmutable('2021-01-01 00:00:00 UTC'), type_array())
        );
    }

    public function test_casting_float_to_array() : void
    {
        $this->assertEquals(
            [1.1],
            (new ArrayCastingHandler())->value(1.1, type_array())
        );
    }

    public function test_casting_integer_to_array() : void
    {
        $this->assertEquals(
            [1],
            (new ArrayCastingHandler())->value(1, type_array())
        );
    }

    public function test_casting_string_to_array() : void
    {
        $this->assertSame(
            ['items' => ['item' => 1]],
            (new ArrayCastingHandler())->value('{"items":{"item":1}}', type_array())
        );
    }
}
