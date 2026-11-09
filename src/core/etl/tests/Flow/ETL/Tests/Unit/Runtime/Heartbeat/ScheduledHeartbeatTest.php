<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Heartbeat;

use DateTimeImmutable;
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
        $now = $context->clock->now();

        $scheduled->start($now);

        static::assertEquals([1, $now->modify('+120 seconds')], [$scheduled->nextBeat(), $scheduled->nextBeatAt()]);

        $context->seconds(130);
        $scheduled->beat(
            new Pulse($scheduled->nextBeat(), 130, 0, 'sort', 0, 1, 1, 0),
            $context->clock,
            $context->logger,
        );

        static::assertEquals([2, $now->modify('+240 seconds')], [$scheduled->nextBeat(), $scheduled->nextBeatAt()]);

        $scheduled->start($now);

        static::assertSame(1, $scheduled->nextBeat());
    }

    public function test_the_next_beat_keeps_the_microseconds_of_the_start(): void
    {
        $scheduled = new ScheduledHeartbeat(new RecordingHeartbeat(), 1);

        $scheduled->start(new DateTimeImmutable('2026-01-01 12:00:00.400'));

        static::assertEquals(new DateTimeImmutable('2026-01-01 12:00:01.400'), $scheduled->nextBeatAt());
    }

    public function test_a_beat_that_ends_late_keeps_the_next_beat_on_the_grid(): void
    {
        $context = new HeartbeatContext();
        $scheduled = new ScheduledHeartbeat(new RecordingHeartbeat(), 1);
        $scheduled->start(new DateTimeImmutable('2026-01-01 12:00:00.400'));
        $context->clock->set(new DateTimeImmutable('2026-01-01 12:00:01.440'));

        $scheduled->beat(new Pulse(1, 1, 0, 'sort', 0, 1, 1, 0), $context->clock, $context->logger);

        static::assertEquals(new DateTimeImmutable('2026-01-01 12:00:02.400'), $scheduled->nextBeatAt());
    }

    public function test_a_beat_past_its_next_due_time_schedules_from_the_end_of_the_callback(): void
    {
        $context = new HeartbeatContext();
        $scheduled = new ScheduledHeartbeat(new RecordingHeartbeat(), 1);
        $scheduled->start(new DateTimeImmutable('2026-01-01 12:00:00.400'));
        $context->clock->set(new DateTimeImmutable('2026-01-01 12:00:05.900'));

        $scheduled->beat(new Pulse(1, 5, 0, 'sort', 0, 1, 1, 0), $context->clock, $context->logger);

        static::assertEquals(new DateTimeImmutable('2026-01-01 12:00:06.900'), $scheduled->nextBeatAt());
    }
}
