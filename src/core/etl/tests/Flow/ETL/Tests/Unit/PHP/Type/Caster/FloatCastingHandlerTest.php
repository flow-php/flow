<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\type_float;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\PHP\Type\Caster\FloatCastingHandler;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class FloatCastingHandlerTest extends TestCase
{
    public static function float_castable_data_provider() : \Generator
    {
        yield 'string' => ['string', 0.0];
        yield 'int' => [1, 1.0];
        yield 'float' => [1.1, 1.1];
        yield 'bool' => [true, 1.0];
        yield 'array' => [[1, 2, 3], 1.0];
        yield 'DateTimeInterface' => [new \DateTimeImmutable('2021-01-01 00:00:00'), 1609459200000000.0];
        yield 'DateInterval' => [new \DateInterval('P1D'), 86400000000.0];
    }

    #[DataProvider('float_castable_data_provider')]
    public function test_casting_different_data_types_to_float(mixed $value, float $expected) : void
    {
        $this->assertSame($expected, (new FloatCastingHandler())->value($value, type_float(), Caster::default()));
    }
}
