<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster, caster_options, type_uuid};
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster\UuidCastingHandler;
use Flow\ETL\PHP\Value\Uuid;
use Flow\ETL\Tests\FlowTestCase;

final class UuidCastingHandlerTest extends FlowTestCase
{
    public function test_casting_integer_to_uuid() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "integer" into "uuid" type');

        (new UuidCastingHandler())->value(1, type_uuid(), caster(), caster_options());
    }

    public function test_casting_ramsey_uuid_to_uuid() : void
    {
        self::assertEquals(
            new Uuid('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'),
            (new UuidCastingHandler())->value(\Ramsey\Uuid\Uuid::fromString('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'), type_uuid(), caster(), caster_options())
        );
    }

    public function test_casting_string_to_uuid() : void
    {
        self::assertEquals(
            new Uuid('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'),
            (new UuidCastingHandler())->value('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e', type_uuid(), caster(), caster_options())
        );
    }

    public function test_casting_xml_element_to_uuid() : void
    {
        $uuid = \Ramsey\Uuid\Uuid::fromString('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e')->toString();

        self::assertEquals(
            $uuid,
            (new UuidCastingHandler())->value(new \DOMElement('element', $uuid), type_uuid(), caster(), caster_options())
        );
    }
}
