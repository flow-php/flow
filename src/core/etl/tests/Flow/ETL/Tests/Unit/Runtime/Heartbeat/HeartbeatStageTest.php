<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Heartbeat;

use Flow\ETL\Runtime\Heartbeat\HeartbeatStage;
use Flow\ETL\Runtime\Heartbeat\ScheduledHeartbeat;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Double\RecordingHeartbeat;
use Flow\ETL\Tests\FlowTestCase;
use RuntimeException;

final class HeartbeatStageTest extends FlowTestCase
{
    public function test_accumulates_its_own_rows(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $stage = new HeartbeatStage('sort', $context->throttle(new ScheduledHeartbeat($recording, 1)));

        $stage->advanced(5);
        $context->seconds(1);
        $stage->advanced(7);

        static::assertSame(['sort', 12], [$recording->pulses()[0]->stage, $recording->pulses()[0]->stageRows]);
    }

    public function test_forwards_reported_totals_unchanged(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $stage = new HeartbeatStage('parallel', $context->throttle(new ScheduledHeartbeat($recording, 1)));

        $context->seconds(1);
        $stage->reported(500, 'join', 120, 1000, 9_000_000_000);

        $pulse = $recording->pulses()[0];
        static::assertSame([500, 'join', 120, 9_000_000_000], [
            $pulse->rowsRead,
            $pulse->stage,
            $pulse->stageRows,
            $pulse->peakMemoryBytes,
        ]);
    }

    public function test_a_report_replaces_its_own_previous_report(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $stage = new HeartbeatStage('parallel', $context->throttle(new ScheduledHeartbeat($recording, 1)));

        $stage->reported(500, 'join', 120, 1_000_000_000_000, 0);
        $context->seconds(1);
        $stage->reported(700, 'join', 150, 7, 0);

        $pulse = $recording->pulses()[0];
        static::assertSame([700, 150], [$pulse->rowsRead, $pulse->stageRows]);
        static::assertLessThan(1_000_000_000_000, $pulse->memoryBytes);
    }

    public function test_ended_neither_beats_nor_throws_and_the_next_due_check_does(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $recording = new RecordingHeartbeat(throws: $failure);
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 1));
        $context->seconds(1);

        (new HeartbeatStage('parallel', $throttle))->ended(500, 'join', 120, 1000, 0);

        static::assertSame([], $recording->pulses());

        try {
            $throttle->beatWhenDue();
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }
    }

    public function test_ended_totals_show_up_in_the_next_pulse(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 1));
        $stage = new HeartbeatStage('parallel', $throttle);

        $stage->reported(300, 'join', 100, 1000, 0);
        $stage->ended(500, 'join', 120, 1000, 0);
        $context->seconds(1);
        $throttle->beatWhenDue();

        static::assertSame([500, 'join', 120], [
            $recording->pulses()[0]->rowsRead,
            $recording->pulses()[0]->stage,
            $recording->pulses()[0]->stageRows,
        ]);
    }

    public function test_reported_still_beats_when_due(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $stage = new HeartbeatStage('parallel', $context->throttle(new ScheduledHeartbeat($recording, 1)));
        $context->seconds(1);

        $stage->reported(500, 'join', 120, 1000, 0);

        static::assertCount(1, $recording->pulses());
    }
}
