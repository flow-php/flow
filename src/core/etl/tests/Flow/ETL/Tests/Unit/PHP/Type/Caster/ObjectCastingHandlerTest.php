<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\type_object;
use Flow\ETL\PHP\Type\Caster\ObjectCastingHandler;
use PHPUnit\Framework\TestCase;

final class ObjectCastingHandlerTest extends TestCase
{
    public function test_casting_string_to_object() : void
    {
        $this->assertEquals(
            (object) ['foo' => 'bar'],
            (new ObjectCastingHandler())->value((object) ['foo' => 'bar'], type_object(\stdClass::class))
        );
        $this->assertInstanceOf(
            \stdClass::class,
            (new ObjectCastingHandler())->value((object) ['foo' => 'bar'], type_object(\stdClass::class))
        );
    }
}
