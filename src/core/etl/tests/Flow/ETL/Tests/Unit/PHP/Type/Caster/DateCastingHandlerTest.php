<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster, caster_options, type_date};
use Flow\ETL\PHP\Type\Caster\{DateCastingHandler};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class DateCastingHandlerTest extends FlowTestCase
{
    public static function date_castable_data_provider() : \Generator
    {
        yield 'string' => ['2021-01-01 00:00:00', new \DateTimeImmutable('2021-01-01 00:00:00')];
        yield 'int' => [1609459200, new \DateTimeImmutable('2021-01-01 00:00:00')];
        yield 'float' => [1609459200.0, new \DateTimeImmutable('2021-01-01 00:00:00')];
        yield 'bool' => [true, new \DateTimeImmutable('1970-01-01 00:00:00')];
        yield 'DateTimeInterface' => [new \DateTimeImmutable('2021-01-01 15:00:00'), new \DateTimeImmutable('2021-01-01 00:00:00')];
        yield 'DateInterval' => [new \DateInterval('P1D'), new \DateTimeImmutable('1970-01-02 00:00:00')];
        yield 'DOMElement' => [new \DOMElement('element', '2021-01-01 12:32:00'), new \DateTimeImmutable('2021-01-01 00:00:00')];
    }

    #[DataProvider('date_castable_data_provider')]
    public function test_casting_different_data_types_to_date(mixed $value, \DateTimeImmutable $expected) : void
    {
        self::assertEquals($expected, (new DateCastingHandler())->value($value, type_date(), caster(), caster_options()));
    }
}
