<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster, caster_options, type_datetime};
use Flow\ETL\PHP\Type\Caster\DateTimeCastingHandler;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class DateTimeCastingHandlerTest extends FlowTestCase
{
    public static function datetime_castable_data_provider() : \Generator
    {
        yield 'string' => ['2021-01-01 00:00:00', new \DateTimeImmutable('2021-01-01 00:00:00')];
        yield 'int' => [1609459200, new \DateTimeImmutable('2021-01-01 00:00:00')];
        yield 'float' => [1609459200.0, new \DateTimeImmutable('2021-01-01 00:00:00')];
        yield 'bool' => [true, new \DateTimeImmutable('1970-01-01 00:00:01')];
        yield 'DateTimeInterface' => [new \DateTimeImmutable('2021-01-01 00:00:00'), new \DateTimeImmutable('2021-01-01 00:00:00')];
        yield 'DateInterval' => [new \DateInterval('P1D'), new \DateTimeImmutable('1970-01-02 00:00:00')];
        yield 'DOMElement' => [new \DOMElement('element', '2021-01-01 00:00:00'), new \DateTimeImmutable('2021-01-01 00:00:00')];
    }

    #[DataProvider('datetime_castable_data_provider')]
    public function test_casting_different_data_types_to_datetime(mixed $value, \DateTimeImmutable $expected) : void
    {
        self::assertEquals($expected, (new DateTimeCastingHandler())->value($value, type_datetime(), caster(), caster_options()));
    }
}
