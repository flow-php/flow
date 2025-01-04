<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster, caster_options, type_object};
use Flow\ETL\PHP\Type\Caster\ObjectCastingHandler;
use Flow\ETL\Tests\FlowTestCase;

final class ObjectCastingHandlerTest extends FlowTestCase
{
    public function test_casting_string_to_object() : void
    {
        self::assertEquals(
            (object) ['foo' => 'bar'],
            (new ObjectCastingHandler())->value((object) ['foo' => 'bar'], type_object(\stdClass::class), caster(), caster_options())
        );
        self::assertInstanceOf(
            \stdClass::class,
            (new ObjectCastingHandler())->value((object) ['foo' => 'bar'], type_object(\stdClass::class), caster(), caster_options())
        );
    }
}
