<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster, caster_options, type_string};
use Flow\ETL\Exception\CastingException;
use Flow\ETL\PHP\Type\Caster\StringCastingHandler;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class StringCastingHandlerTest extends FlowTestCase
{
    public static function string_castable_data_provider() : \Generator
    {
        yield 'string' => ['string', 'string'];
        yield 'int' => [1, '1'];
        yield 'float' => [1.1, '1.1'];
        yield 'bool' => [true, 'true'];
        yield 'array' => [[1, 2, 3], '[1,2,3]'];
        yield 'DateTimeInterface' => [new \DateTimeImmutable('2021-01-01 00:00:00'), '2021-01-01T00:00:00+00:00'];
        yield 'Stringable' => [new class() implements \Stringable {
            public function __toString() : string
            {
                return 'stringable';
            }
        }, 'stringable'];
        yield 'DOMDocument' => [new \DOMDocument(), '<?xml version="1.0"?>'];
        yield 'DOMElement' => [new \DOMElement('element'), '<element/>'];
    }

    #[DataProvider('string_castable_data_provider')]
    public function test_casting_different_data_types_to_string(mixed $value, string $expected) : void
    {
        self::assertSame($expected, \trim((new StringCastingHandler())->value($value, type_string(), caster(), caster_options())));
    }

    public function test_casting_object_to_string() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "object" into "string" type');

        (new StringCastingHandler())->value(new class() {}, type_string(), caster(), caster_options());
    }
}
