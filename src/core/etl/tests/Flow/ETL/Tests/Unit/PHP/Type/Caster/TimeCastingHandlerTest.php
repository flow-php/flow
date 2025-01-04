<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster, caster_options, type_time};
use Flow\ETL\PHP\Type\Caster\TimeCastingHandler;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class TimeCastingHandlerTest extends FlowTestCase
{
    public static function time_castable_data_provider() : \Generator
    {
        yield 'string' => ['PT1S', new \DateInterval('PT1S')];
        yield 'datetime' => [new \DateTimeImmutable('2021-01-01 00:00:01'), new \DateInterval('PT1S')];
        yield 'date' => [new \DateTimeImmutable('2021-01-01'), new \DateInterval('PT0S')];
        yield 'time' => [new \DateInterval('PT10S'), new \DateInterval('PT10S')];
    }

    #[DataProvider('time_castable_data_provider')]
    public function test_casting_different_time_types_to_time(mixed $value, \DateInterval $expextedInterval) : void
    {
        self::assertEquals($expextedInterval, (new TimeCastingHandler())->value($value, type_time(), caster(), caster_options()));
    }
}
