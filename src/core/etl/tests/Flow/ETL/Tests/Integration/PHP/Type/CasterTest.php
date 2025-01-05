<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\PHP\Type;

use function Flow\ETL\DSL\{caster,
    type_boolean,
    type_datetime,
    type_integer,
    type_json,
    type_null,
    type_string,
    type_uuid,
    type_xml};
use Flow\ETL\PHP\Value\Uuid;
use Flow\ETL\Tests\FlowTestCase;

final class CasterTest extends FlowTestCase
{
    public function test_casting_array_to_json() : void
    {
        self::assertSame(
            '{"items":{"item":1}}',
            caster()->to(type_json())->value(['items' => ['item' => 1]])
        );
    }

    public function test_casting_string_to_datetime() : void
    {
        self::assertSame(
            '2021-01-01 00:00:00.000000',
            caster()->to(type_datetime())->value('2021-01-01 00:00:00 UTC')->format('Y-m-d H:i:s.u')
        );
    }

    public function test_casting_string_to_uuid() : void
    {
        self::assertEquals(
            new Uuid('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'),
            caster()->to(type_uuid())->value('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e')
        );
    }

    public function test_casting_string_to_xml() : void
    {
        self::assertSame(
            '<?xml version="1.0"?>' . "\n" . '<items><item>1</item></items>' . "\n",
            caster()->to(type_xml())->value('<items><item>1</item></items>')->saveXML()
        );
    }

    public function test_casting_to_boolean() : void
    {
        self::assertTrue(
            caster()->to(type_boolean())->value('true')
        );
    }

    public function test_casting_to_integer() : void
    {
        self::assertSame(
            1,
            caster()->to(type_integer())->value('1')
        );
    }

    public function test_casting_to_string() : void
    {
        self::assertSame(
            '1',
            caster()->to(type_string())->value(1)
        );
    }

    public function test_casting_values_to_null() : void
    {
        self::assertNull(
            caster()->to(type_null())->value('qweqwqw')
        );
    }
}
