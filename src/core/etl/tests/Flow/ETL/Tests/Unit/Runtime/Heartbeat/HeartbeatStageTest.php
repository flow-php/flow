<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Heartbeat;

use Flow\ETL\Runtime\Heartbeat\HeartbeatStage;
use Flow\ETL\Runtime\Heartbeat\ScheduledHeartbeat;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Double\RecordingHeartbeat;
use Flow\ETL\Tests\FlowTestCase;

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
}
