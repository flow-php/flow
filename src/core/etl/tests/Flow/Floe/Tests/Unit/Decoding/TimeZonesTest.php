<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\TimeZones;
use PHPUnit\Framework\TestCase;

final class TimeZonesTest extends TestCase
{
    public function test_returns_same_instance_for_repeated_name(): void
    {
        $timeZones = new TimeZones();

        static::assertSame('UTC', $timeZones->get('UTC')->getName());
        static::assertSame($timeZones->get('UTC'), $timeZones->get('UTC'));
    }
}
