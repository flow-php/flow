<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Heartbeat;

use ArrayObject;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Runtime\Heartbeat\HeartbeatFailure;
use Flow\ETL\Runtime\Heartbeat\Pulse;
use Flow\ETL\Runtime\Heartbeat\ScheduledHeartbeat;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Double\RecordingHeartbeat;
use Flow\ETL\Tests\Double\ReportingHeartbeat;
use Flow\ETL\Tests\FlowTestCase;
use RuntimeException;

use function array_map;
use function array_slice;
use function iterator_to_array;
use function range;

final class HeartbeatThrottleTest extends FlowTestCase
{
    public function test_does_not_beat_before_the_interval(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));

        $context->seconds(119);
        $throttle->stage('sort')->advanced(10);

        static::assertSame([], $recording->pulses());
    }

    public function test_beats_once_the_interval_passed(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));

        $context->seconds(120);
        $throttle->stage('sort')->advanced(10);

        static::assertCount(1, $recording->pulses());
        $pulse = $recording->pulses()[0];
        static::assertSame([1, 120, 0, 'sort', 10], [
            $pulse->beat,
            $pulse->elapsedSeconds,
            $pulse->rowsRead,
            $pulse->stage,
            $pulse->stageRows,
        ]);
        static::assertGreaterThan(0, $pulse->memoryBytes);
        static::assertGreaterThan(0, $pulse->peakMemoryBytes);
    }

    public function test_numbers_beats_and_waits_a_full_interval_after_each(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $sort = $context->throttle(new ScheduledHeartbeat($recording, 120))->stage('sort');

        $context->seconds(120);
        $sort->advanced(1);
        $context->seconds(1);
        $sort->advanced(1);
        $context->seconds(119);
        $sort->advanced(1);

        static::assertSame([[1, 120], [2, 240]], HeartbeatContext::beats($recording));
    }

    public function test_pulse_names_the_last_stage_that_reported(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));

        $throttle->stage('sort')->advanced(10);
        $context->seconds(120);
        $throttle->stage('join')->advanced(3);

        static::assertSame(['join', 3], [$recording->pulses()[0]->stage, $recording->pulses()[0]->stageRows]);
    }

    public function test_counts_rows_read_from_the_source(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $corrupted = new CorruptedRecordException(null, 2, null, 'not a number', 'x');
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $reading = $throttle->reading(HeartbeatContext::batches(3, $corrupted));

        $reading->current();
        $reading->next();
        static::assertSame($corrupted, $reading->current());
        $reading->next();
        $context->seconds(120);
        $reading->next();

        static::assertSame([], $recording->pulses());

        $throttle->beatWhenDue();

        static::assertSame(['read', 30, 30], [
            $recording->pulses()[0]->stage,
            $recording->pulses()[0]->stageRows,
            $recording->pulses()[0]->rowsRead,
        ]);
    }

    public function test_a_stop_signal_reaches_the_source(): void
    {
        $stopped = new ArrayObject();
        $generator = (new HeartbeatContext())
            ->throttle(new ScheduledHeartbeat(new RecordingHeartbeat(), 120))
            ->reading(HeartbeatContext::stoppable($stopped));

        $generator->current();
        $generator->send(Signal::STOP);

        static::assertSame([Signal::STOP], $stopped->getArrayCopy());
        static::assertFalse($generator->valid());
    }

    public function test_folds_totals_reported_from_other_processes(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $join = $throttle->stage('join');
        $parallel = $throttle->stage('parallel');

        iterator_to_array($throttle->reading(HeartbeatContext::batches(1)), false);
        $join->advanced(4);
        $context->seconds(120);
        $parallel->reported(500, 'sort', 1200, 1000, 9_000_000_000);
        $context->seconds(120);
        $parallel->reported(700, 'group_by', 300, 2000, 8_000_000_000);
        $context->seconds(120);
        $join->advanced(1);

        [$first, $second, $third] = $recording->pulses();
        static::assertSame([510, 'sort', 1200, 9_000_000_000], [
            $first->rowsRead,
            $first->stage,
            $first->stageRows,
            $first->peakMemoryBytes,
        ]);
        static::assertGreaterThan(1000, $first->memoryBytes);
        static::assertSame([710, 'group_by', 300, 8_000_000_000], [
            $second->rowsRead,
            $second->stage,
            $second->stageRows,
            $second->peakMemoryBytes,
        ]);
        static::assertSame([710, 'join', 5], [$third->rowsRead, $third->stage, $third->stageRows]);
    }

    public function test_a_heartbeat_that_reports_progress_is_not_called_again_inside_its_own_beat(): void
    {
        $context = new HeartbeatContext();
        $heartbeat = new ReportingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($heartbeat, 120));
        $heartbeat->reportsTo = $throttle->stage('sort');

        $context->seconds(120);
        $throttle->stage('sort')->advanced(1);

        static::assertSame(1, $heartbeat->beats);
    }

    public function test_stop_rethrows_the_heartbeat_failure(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $sort = $context
            ->throttle(new ScheduledHeartbeat(new RecordingHeartbeat(throws: $failure), 120))
            ->stage('sort');
        $context->seconds(120);

        try {
            $sort->advanced(1);
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }
    }

    public function test_log_logs_the_heartbeat_failure_and_keeps_beating(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat(throws: new RuntimeException('lease lost'));
        $sort = $context->throttle(new ScheduledHeartbeat($recording, 120, HeartbeatFailure::log))->stage('sort');

        $context->seconds(120);
        $sort->advanced(1);

        static::assertSame(1, $context->errorLogs());

        $context->seconds(120);
        $sort->advanced(1);

        static::assertSame([[1, 120], [2, 240]], HeartbeatContext::beats($recording));
    }

    public function test_a_nested_run_does_not_restart_the_clock(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $sort = $throttle->stage('sort');

        $throttle->started();
        $context->seconds(60);
        $throttle->started();
        $throttle->finished();
        $context->seconds(60);
        $sort->advanced(1);
        $throttle->finished();
        $context->seconds(30);
        $throttle->started();
        $context->seconds(120);
        $sort->advanced(1);

        static::assertSame([[1, 120], [1, 120]], HeartbeatContext::beats($recording));
    }

    public function test_beats_each_heartbeat_on_its_own_interval(): void
    {
        $context = new HeartbeatContext();
        $calls = new ArrayObject();
        $a = new RecordingHeartbeat('a', calls: $calls);
        $b = new RecordingHeartbeat('b', calls: $calls);
        $sort = $context->throttle(new ScheduledHeartbeat($a, 120), new ScheduledHeartbeat($b, 1))->stage('sort');

        foreach (range(1, 240) as $_) {
            $context->seconds(1);
            $sort->advanced(1);
        }

        static::assertSame([[1, 120], [2, 240]], HeartbeatContext::beats($a));
        static::assertSame(range(1, 240), array_map(static fn(Pulse $pulse): int => $pulse->beat, $b->pulses()));
        static::assertSame([['b', 119], ['a', 1], ['b', 120]], array_slice($calls->getArrayCopy(), 118, 3));
    }

    public function test_a_logging_heartbeat_failure_does_not_stop_the_others(): void
    {
        $context = new HeartbeatContext();
        $a = new RecordingHeartbeat('a', new RuntimeException('console closed'));
        $b = new RecordingHeartbeat('b');
        $sort = $context
            ->throttle(new ScheduledHeartbeat($a, 1, HeartbeatFailure::log), new ScheduledHeartbeat($b, 1))
            ->stage('sort');

        $context->seconds(1);
        $sort->advanced(1);

        static::assertSame(1, $context->errorLogs());
        static::assertSame([[1, 1]], HeartbeatContext::beats($b));

        $context->seconds(1);
        $sort->advanced(1);

        static::assertSame([[1, 1], [2, 2]], HeartbeatContext::beats($a));
    }

    public function test_a_stopping_heartbeat_failure_skips_the_heartbeats_after_it(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $b = new RecordingHeartbeat('b');
        $sort = $context
            ->throttle(new ScheduledHeartbeat(new RecordingHeartbeat('a', $failure), 1), new ScheduledHeartbeat($b, 1))
            ->stage('sort');
        $context->seconds(1);

        try {
            $sort->advanced(1);
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertSame([], $b->pulses());
    }
}
