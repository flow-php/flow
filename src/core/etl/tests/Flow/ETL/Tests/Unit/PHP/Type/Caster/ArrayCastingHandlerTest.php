<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster, caster_options, type_array};
use Flow\ETL\PHP\Type\Caster\ArrayCastingHandler;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayCastingHandlerTest extends FlowTestCase
{
    public function test_casting_boolean_to_array() : void
    {
        self::assertEquals(
            [true],
            (new ArrayCastingHandler())->value(true, type_array(), caster(), caster_options())
        );
    }

    public function test_casting_datetime_to_array() : void
    {
        self::assertEquals(
            ['date' => '2021-01-01 00:00:00.000000', 'timezone_type' => 3, 'timezone' => 'UTC'],
            (new ArrayCastingHandler())->value(new \DateTimeImmutable('2021-01-01 00:00:00 UTC'), type_array(), caster(), caster_options())
        );
    }

    public function test_casting_float_to_array() : void
    {
        self::assertEquals(
            [1.1],
            (new ArrayCastingHandler())->value(1.1, type_array(), caster(), caster_options())
        );
    }

    public function test_casting_integer_to_array() : void
    {
        self::assertEquals(
            [1],
            (new ArrayCastingHandler())->value(1, type_array(), caster(), caster_options())
        );
    }

    public function test_casting_string_to_array() : void
    {
        self::assertSame(
            ['items' => ['item' => 1]],
            (new ArrayCastingHandler())->value('{"items":{"item":1}}', type_array(), caster(), caster_options())
        );
    }

    public function test_casting_xml_document_to_array() : void
    {
        $xml = new \DOMDocument();
        $xml->loadXML($xmlString = '<root><foo baz="buz">bar</foo></root>');

        self::assertSame(
            ['root' => ['foo' => ['@attributes' => ['baz' => 'buz'], '@value' => 'bar']]],
            (new ArrayCastingHandler())->value($xml, type_array(), caster(), caster_options())
        );
    }
}
