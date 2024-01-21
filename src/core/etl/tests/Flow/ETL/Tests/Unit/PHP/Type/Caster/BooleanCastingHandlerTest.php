<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\type_boolean;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\PHP\Type\Caster\BooleanCastingHandler;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class BooleanCastingHandlerTest extends TestCase
{
    public static function boolean_castable_data_provider() : \Generator
    {
        yield 'string' => ['string', true];
        yield 'string true' => ['true', true];
        yield 'string 1' => ['1', true];
        yield 'string yes' => ['yes', true];
        yield 'string on' => ['on', true];
        yield 'string false' => ['false', false];
        yield 'string 0' => ['0', false];
        yield 'string no' => ['no', false];
        yield 'string off' => ['off', false];
        yield 'int' => [1, true];
        yield 'float' => [1.1, true];
        yield 'bool' => [true, true];
        yield 'array' => [[1, 2, 3], true];
        yield 'DateTimeInterface' => [new \DateTimeImmutable('2021-01-01 00:00:00'), true];
        yield 'DateInterval' => [new \DateInterval('P1D'), true];
        yield 'DOMDocument' => [new \DOMDocument(), true];
    }

    #[DataProvider('boolean_castable_data_provider')]
    public function test_casting_different_data_types_to_integer(mixed $value, bool $expected) : void
    {
        $this->assertSame($expected, (new BooleanCastingHandler())->value($value, type_boolean(), Caster::default()));
    }
}
