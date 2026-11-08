<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Heartbeat;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Runtime\Heartbeat\Pulse;
use Flow\ETL\Runtime\Heartbeat\ScheduledHeartbeat;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Double\RecordingHeartbeat;
use Flow\ETL\Tests\FlowTestCase;

final class ScheduledHeartbeatTest extends FlowTestCase
{
    public function test_refuses_an_interval_below_one_second(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Heartbeat interval must be at least 1 second, got 0');

        new ScheduledHeartbeat(new RecordingHeartbeat(), 0);
    }

    public function test_counts_its_own_beats(): void
    {
        $context = new HeartbeatContext();
        $scheduled = new ScheduledHeartbeat(new RecordingHeartbeat(), 120);
        $now = $context->clock->now()->getTimestamp();

        $scheduled->start($now);

        static::assertSame([1, $now + 120], [$scheduled->nextBeat(), $scheduled->nextBeatAt()]);

        $context->seconds(130);
        $scheduled->beat(new Pulse($scheduled->nextBeat(), 130, 0, 'sort', 0, 1, 1), $context->clock, $context->logger);

        static::assertSame([2, $now + 250], [$scheduled->nextBeat(), $scheduled->nextBeatAt()]);

        $scheduled->start($now);

        static::assertSame(1, $scheduled->nextBeat());
    }
}
