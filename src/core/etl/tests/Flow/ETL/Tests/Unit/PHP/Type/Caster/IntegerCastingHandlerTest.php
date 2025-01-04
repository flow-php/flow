<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster, caster_options, type_integer};
use Flow\ETL\PHP\Type\Caster\IntegerCastingHandler;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class IntegerCastingHandlerTest extends FlowTestCase
{
    public static function integer_castable_data_provider() : \Generator
    {
        yield 'string' => ['string', 0];
        yield 'int' => [1, 1];
        yield 'float' => [1.1, 1];
        yield 'bool' => [true, 1];
        yield 'array' => [[1, 2, 3], 1];
        yield 'DateTimeInterface' => [new \DateTimeImmutable('2021-01-01 00:00:00'), 1609459200000000];
        yield 'DateInterval' => [new \DateInterval('P1D'), 86400000000];
        yield 'DOMElement' => [new \DOMElement('element', '1'), 1];
    }

    #[DataProvider('integer_castable_data_provider')]
    public function test_casting_different_data_types_to_integer(mixed $value, int $expected) : void
    {
        self::assertSame($expected, (new IntegerCastingHandler())->value($value, type_integer(), caster(), caster_options()));
    }
}
