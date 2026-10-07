<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Type\RoundTripPrecision;
use PHPUnit\Framework\TestCase;

use function ini_get;
use function ini_set;
use function json_encode;

final class RoundTripPrecisionTest extends TestCase
{
    public function test_force_writes_the_shortest_round_trip_text_until_restore(): void
    {
        $previous = (string) ini_get('serialize_precision');
        ini_set('serialize_precision', '17');

        try {
            $precision = new RoundTripPrecision();
            $precision->force();

            static::assertSame('-1', ini_get('serialize_precision'));
            static::assertSame('0.1', json_encode(0.1));

            $precision->restore();

            static::assertSame('17', ini_get('serialize_precision'));
            static::assertSame('0.10000000000000001', json_encode(0.1));
        } finally {
            ini_set('serialize_precision', $previous);
        }
    }

    public function test_force_changes_nothing_when_the_ini_is_already_round_trip(): void
    {
        $previous = (string) ini_get('serialize_precision');
        ini_set('serialize_precision', '-1');

        try {
            $precision = new RoundTripPrecision();
            $precision->force();
            ini_set('serialize_precision', '5');
            $precision->restore();

            static::assertSame('5', ini_get('serialize_precision'));
        } finally {
            ini_set('serialize_precision', $previous);
        }
    }

    public function test_restore_is_applied_once(): void
    {
        $previous = (string) ini_get('serialize_precision');
        ini_set('serialize_precision', '17');

        try {
            $precision = new RoundTripPrecision();
            $precision->force();
            $precision->restore();
            ini_set('serialize_precision', '9');
            $precision->restore();

            static::assertSame('9', ini_get('serialize_precision'));
        } finally {
            ini_set('serialize_precision', $previous);
        }
    }
}
