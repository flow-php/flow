<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\type_uuid;
use Flow\ETL\PHP\Type\Caster\UuidCastingHandler;
use Flow\ETL\Row\Entry\Type\Uuid;
use PHPUnit\Framework\TestCase;

final class UuidCastingHandlerTest extends TestCase
{
    public function test_casting_integer_to_uuid() : void
    {
        $this->expectException(\Flow\ETL\Exception\CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "integer" into "uuid" type');

        (new UuidCastingHandler())->value(1, type_uuid());
    }

    public function test_casting_ramsey_uuid_to_uuid() : void
    {
        $this->assertEquals(
            new Uuid('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'),
            (new UuidCastingHandler())->value(\Ramsey\Uuid\Uuid::fromString('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'), type_uuid())
        );
    }

    public function test_casting_string_to_uuid() : void
    {
        $this->assertEquals(
            new Uuid('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e'),
            (new UuidCastingHandler())->value('6c2f6e0e-8d8e-4e9e-8f0e-5a2d9c1c4f6e', type_uuid())
        );
    }
}
