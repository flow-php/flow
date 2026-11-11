<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Heartbeat;

use ArrayObject;
use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\StageProgress;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Runtime\Heartbeat\HeartbeatFailure;
use Flow\ETL\Runtime\Heartbeat\HeartbeatThrottle;
use Flow\ETL\Runtime\Heartbeat\Pulse;
use Flow\ETL\Runtime\Heartbeat\ScheduledHeartbeat;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Tests\Double\RecordingHeartbeat;
use Flow\ETL\Tests\Double\ReportedMemoryBackend;
use Flow\ETL\Tests\Double\ReportingHeartbeat;
use Flow\ETL\Tests\Double\SteppingClock;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\Unit;
use Generator;
use RuntimeException;

use function array_map;
use function array_slice;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function max;
use function memory_get_peak_usage;
use function range;
use function restore_error_handler;
use function set_error_handler;

final class HeartbeatThrottleTest extends FlowTestCase
{
    public function test_does_not_beat_before_the_interval(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));

        $context->seconds(119);
        $throttle->stage(StageName::sort)->advanced(10);

        static::assertSame([], $recording->pulses());
    }

    public function test_beats_once_the_interval_passed(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));

        $context->seconds(120);
        $throttle->stage(StageName::sort)->advanced(10);

        static::assertCount(1, $recording->pulses());
        $pulse = $recording->pulses()[0];
        static::assertSame([1, 120.0, 0, 'sort', 10], [
            $pulse->beat,
            $pulse->elapsedSeconds,
            $pulse->rowsRead,
            $pulse->stages[0]->name->value,
            $pulse->stages[0]->count,
        ]);
        static::assertGreaterThan(0, $pulse->memoryBytes);
        static::assertGreaterThan(0, $pulse->peakMemoryBytes);
    }

    public function test_numbers_beats_and_waits_a_full_interval_after_each(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $sort = $context->throttle(new ScheduledHeartbeat($recording, 120))->stage(StageName::sort);

        $context->seconds(120);
        $sort->advanced(1);
        $context->seconds(1);
        $sort->advanced(1);
        $context->seconds(119);
        $sort->advanced(1);

        static::assertSame([[1, 120.0], [2, 240.0]], HeartbeatContext::beats($recording));
    }

    public function test_counts_rows_read_from_the_source(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $corrupted = new CorruptedRecordException(null, 2, null, 'not a number', 'x');
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $reading = $throttle->reading(HeartbeatContext::batches(3, $corrupted), $throttle->stage(StageName::read));

        $reading->current();
        $reading->next();
        static::assertSame($corrupted, $reading->current());
        $reading->next();
        $context->seconds(120);
        $reading->next();

        static::assertSame([], $recording->pulses());

        $throttle->beatWhenDue();

        static::assertSame(['read', 30, 30], [
            $recording->pulses()[0]->stages[0]->name->value,
            $recording->pulses()[0]->stages[0]->count,
            $recording->pulses()[0]->rowsRead,
        ]);
    }

    public function test_a_stop_signal_reaches_the_source(): void
    {
        $stopped = new ArrayObject();
        $throttle = (new HeartbeatContext())->throttle(new ScheduledHeartbeat(new RecordingHeartbeat(), 120));
        $generator = $throttle->reading(HeartbeatContext::stoppable($stopped), $throttle->stage(StageName::read));

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
        $join = $throttle->stage(StageName::join);
        $parallel = $throttle->stage(StageName::parallel);

        iterator_to_array($throttle->reading(HeartbeatContext::batches(1), $throttle->stage(StageName::read)), false);
        $join->advanced(4);
        $context->seconds(120);
        $parallel->reported(
            500,
            [new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 1200, null, 0.0, null)],
            1000,
            9_000_000_000,
            false,
        );
        $context->seconds(120);
        $parallel->reported(
            700,
            [
                new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 1200, null, 0.0, null),
                new StageProgress(8, StageName::group_by, ProgressUnit::groups, null, 300, null, 0.0, null),
            ],
            2000,
            10_000_000_000,
            false,
        );
        $context->seconds(120);
        $join->advanced(1);

        [$first, $second, $third] = $recording->pulses();
        static::assertSame([510, 9_000_000_000], [$first->rowsRead, $first->peakMemoryBytes]);
        static::assertSame(
            [1200],
            array_map(
                static fn(StageProgress $sort): int => $sort->count,
                HeartbeatContext::entries($recording, StageName::sort)[0],
            ),
        );
        static::assertGreaterThan(1000, $first->memoryBytes);
        static::assertSame([710, 10_000_000_000], [$second->rowsRead, $second->peakMemoryBytes]);
        static::assertSame(
            [300],
            array_map(
                static fn(StageProgress $groups): int => $groups->count,
                HeartbeatContext::entries($recording, StageName::group_by)[1],
            ),
        );
        static::assertSame([710, [5]], [
            $third->rowsRead,
            array_map(
                static fn(StageProgress $join): int => $join->count,
                HeartbeatContext::entries($recording, StageName::join)[2],
            ),
        ]);
    }

    public function test_beat_now_beats_every_heartbeat_before_its_interval_and_restarts_it(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));

        iterator_to_array($throttle->reading(HeartbeatContext::batches(2), $throttle->stage(StageName::read)), false);
        $context->seconds(30);
        $throttle->beatNow();
        $context->seconds(100);
        $throttle->beatWhenDue();
        $context->seconds(20);
        $throttle->beatWhenDue();

        static::assertSame([[1, 30.0], [2, 150.0]], HeartbeatContext::beats($recording));
        static::assertSame(20, $recording->pulses()[0]->rowsRead);
    }

    public function test_memory_of_a_launch_drops_out_once_it_reports_zero(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $finishing = $throttle->stage(StageName::parallel);
        $running = $throttle->stage(StageName::parallel);

        $finishing->reported(10, [], 1_000_000_000_000, 0, false);
        $running->reported(10, [], 7, 0, false);
        $context->seconds(120);
        $running->reported(20, [], 7, 0, false);
        $finishing->reported(10, [], 0, 0, false);
        $context->seconds(120);
        $running->reported(30, [], 7, 0, false);

        [$both, $runningOnly] = $recording->pulses();
        static::assertGreaterThan(1_000_000_000_000, $both->memoryBytes);
        static::assertLessThan(1_000_000_000_000, $runningOnly->memoryBytes);
        static::assertSame(40, $runningOnly->rowsRead);
    }

    public function test_a_new_run_resets_the_totals_reported_from_other_processes(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));

        $throttle->stage(StageName::parallel)->reported(
            500,
            [new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 500, null, 0.0, null)],
            1_000_000_000_000,
            9_000_000_000_000,
            false,
        );
        $throttle->started();
        $context->seconds(120);
        $throttle->stage(StageName::parallel)->reported(0, [], 0, 0, false);

        $pulse = $recording->pulses()[0];
        static::assertSame([0, []], [$pulse->rowsRead, $pulse->stages]);
        static::assertLessThan(1_000_000_000_000, $pulse->memoryBytes);
        static::assertLessThan(9_000_000_000_000, $pulse->peakMemoryBytes);
    }

    public function test_a_read_stage_from_before_a_new_run_adds_no_entry_and_its_rows_still_count(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $read = $throttle->stage(StageName::read);
        $read->read(5);
        $throttle->finished();
        $throttle->started();
        $warnings = new ArrayObject();
        set_error_handler(static function (int $level, string $message) use ($warnings): bool {
            $warnings->append($message);

            return true;
        });

        try {
            $read->read(3);
            $throttle->unread(1);
            $throttle->beatNow();
        } finally {
            restore_error_handler();
        }

        static::assertSame([[], 2, []], [
            $warnings->getArrayCopy(),
            $recording->pulses()[0]->rowsRead,
            $recording->pulses()[0]->stages,
        ]);
    }

    public function test_a_run_after_a_larger_run_reports_its_own_peak(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $backend = new ReportedMemoryBackend(Unit::fromMb(300)->inBytes(), reportedPeak: Unit::fromMb(300)->inBytes());
        $throttle = new HeartbeatThrottle($context->clock, $backend, $context->logger, [
            new ScheduledHeartbeat($recording, 120),
        ]);
        $throttle->started();
        $throttle->beatNow();
        $throttle->finished();
        $backend->reported = Unit::fromMb(10)->inBytes();

        $throttle->started();
        $throttle->beatNow();

        [$first, $second] = $recording->pulses();
        static::assertLessThan($first->peakMemoryBytes - Unit::fromMb(200)->inBytes(), $second->peakMemoryBytes);
    }

    public function test_two_stages_of_one_parent_id_in_a_worker_add_up_to_one_entry_that_ends_with_both(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $worker = $context->throttleToParent(new ScheduledHeartbeat($recording, 120));
        $first = $worker->stage(StageName::sort, id: 7);
        $second = $worker->stage(StageName::sort, id: 7);

        $first->advanced(2);
        $second->advanced(3);
        $first->done();
        $worker->beatNow();
        $second->done();
        $worker->beatNow();

        static::assertSame(
            [[[7, 5, false]], [[7, 5, true]]],
            array_map(static fn(Pulse $pulse): array => array_map(static fn(StageProgress $stage): array => [
                $stage->id,
                $stage->count,
                $stage->endedAtSeconds !== null,
            ], $pulse->stages), $recording->pulses()),
        );
    }

    public function test_a_read_stage_for_a_plan_without_one_carries_the_total_it_is_given(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();

        $throttle->readStage(null, from_array([['id' => 1]]), 3)->busy();
        $throttle->beatNow();

        static::assertSame(3, HeartbeatContext::entries($recording, StageName::read)[0][0]->total);
    }

    public function test_planned_lists_every_stage_made_since_the_run_started(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $throttle->stage(StageName::read);
        $throttle->stage(StageName::sort);

        $throttle->planned();
        $throttle->beatNow();

        static::assertSame(
            [['read', 0, null], ['sort', 1, null]],
            array_map(static fn(StageProgress $stage): array => [
                $stage->name->value,
                $stage->id,
                $stage->startedAtSeconds,
            ], $recording->pulses()[0]->stages),
        );
    }

    public function test_a_stage_that_ended_before_planned_is_never_listed(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $throttle->stage(StageName::infer)->done();
        $throttle->stage(StageName::sort);

        $throttle->planned();
        $throttle->beatNow();
        $throttle->completed();

        static::assertSame(
            [['sort'], ['sort']],
            array_map(static fn(Pulse $pulse): array => array_map(
                static fn(StageProgress $stage): string => $stage->name->value,
                $pulse->stages,
            ), $recording->pulses()),
        );
    }

    public function test_a_heartbeat_that_reports_progress_is_not_called_again_inside_its_own_beat(): void
    {
        $context = new HeartbeatContext();
        $heartbeat = new ReportingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($heartbeat, 120));
        $heartbeat->reportsTo = $throttle->stage(StageName::sort);

        $context->seconds(120);
        $throttle->stage(StageName::sort)->advanced(1);

        static::assertSame(1, $heartbeat->beats);
    }

    public function test_stop_rethrows_the_heartbeat_failure(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $sort = $context
            ->throttle(new ScheduledHeartbeat(new RecordingHeartbeat(throws: $failure), 120))
            ->stage(StageName::sort);
        $context->seconds(120);

        try {
            $sort->advanced(1);
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }
    }

    public function test_a_stopped_throttle_calls_no_heartbeat_again(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $recording = new RecordingHeartbeat(throws: $failure);
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $context->seconds(120);

        try {
            $throttle->beatWhenDue();
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        $context->seconds(120);
        $throttle->beatWhenDue();
        $throttle->beatNow();

        static::assertCount(1, $recording->pulses());
        static::assertTrue($throttle->stopped());
    }

    public function test_a_new_run_beats_again_after_a_stop(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $recording = new RecordingHeartbeat(throws: $failure);
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $context->seconds(120);

        try {
            $throttle->beatWhenDue();
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        $throttle->finished();
        $throttle->started();

        static::assertFalse($throttle->stopped());

        $context->seconds(120);

        try {
            $throttle->beatWhenDue();
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertCount(2, $recording->pulses());
    }

    public function test_log_logs_the_heartbeat_failure_and_keeps_beating(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat(throws: new RuntimeException('lease lost'));
        $sort = $context
            ->throttle(new ScheduledHeartbeat($recording, 120, HeartbeatFailure::log))
            ->stage(StageName::sort);

        $context->seconds(120);
        $sort->advanced(1);

        static::assertSame(1, $context->errorLogs());

        $context->seconds(120);
        $sort->advanced(1);

        static::assertSame([[1, 120.0], [2, 240.0]], HeartbeatContext::beats($recording));
    }

    public function test_a_nested_run_does_not_restart_the_clock(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $sort = $throttle->stage(StageName::sort);

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

        static::assertSame([[1, 120.0], [1, 120.0]], HeartbeatContext::beats($recording));
    }

    public function test_beats_each_heartbeat_on_its_own_interval(): void
    {
        $context = new HeartbeatContext();
        $calls = new ArrayObject();
        $a = new RecordingHeartbeat('a', calls: $calls);
        $b = new RecordingHeartbeat('b', calls: $calls);
        $sort = $context
            ->throttle(new ScheduledHeartbeat($a, 120), new ScheduledHeartbeat($b, 1))
            ->stage(StageName::sort);

        foreach (range(1, 240) as $_) {
            $context->seconds(1);
            $sort->advanced(1);
        }

        static::assertSame([[1, 120.0], [2, 240.0]], HeartbeatContext::beats($a));
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
            ->stage(StageName::sort);

        $context->seconds(1);
        $sort->advanced(1);

        static::assertSame(1, $context->errorLogs());
        static::assertSame([[1, 1.0]], HeartbeatContext::beats($b));

        $context->seconds(1);
        $sort->advanced(1);

        static::assertSame([[1, 1.0], [2, 2.0]], HeartbeatContext::beats($a));
    }

    public function test_a_stopping_heartbeat_failure_skips_the_heartbeats_after_it(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $b = new RecordingHeartbeat('b');
        $sort = $context
            ->throttle(new ScheduledHeartbeat(new RecordingHeartbeat('a', $failure), 1), new ScheduledHeartbeat($b, 1))
            ->stage(StageName::sort);
        $context->seconds(1);

        try {
            $sort->advanced(1);
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertSame([], $b->pulses());
    }

    public function test_totals_from_other_processes_never_beat_by_themselves(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 1));
        $context->seconds(1);

        $throttle->othersReported(
            0,
            500,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 120, null, 0.0, null)],
            1000,
            0,
            false,
        );

        static::assertSame([], $recording->pulses());
    }

    public function test_the_first_beat_waits_a_whole_interval_from_a_start_inside_a_second(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $context->clock->set(new DateTimeImmutable('2026-01-01 10:00:00.900'));
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 1));

        $context->clock->set(new DateTimeImmutable('2026-01-01 10:00:01.100'));
        $throttle->beatWhenDue();

        static::assertSame([], $recording->pulses());

        $context->clock->set(new DateTimeImmutable('2026-01-01 10:00:01.900'));
        $throttle->beatWhenDue();

        static::assertSame([[1, 1.0]], HeartbeatContext::beats($recording));
    }

    public function test_elapsed_seconds_are_the_real_time_since_the_start(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $context->clock->set(new DateTimeImmutable('2026-01-01 10:00:00.900'));
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 1));

        $context->clock->set(new DateTimeImmutable('2026-01-01 10:00:02.100'));
        $throttle->beatWhenDue();

        static::assertSame([[1, 1.2]], HeartbeatContext::beats($recording));
    }

    public function test_idle_seconds_count_from_the_last_batch(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 10));

        $context->seconds(3);
        iterator_to_array($throttle->reading(HeartbeatContext::batches(1), $throttle->stage(StageName::read)), false);
        $throttle->beatWhenDue();
        $context->seconds(7);
        $throttle->beatWhenDue();

        static::assertSame(7.0, $recording->pulses()[0]->idleSeconds);
    }

    public function test_an_unchanged_report_keeps_idle_seconds_growing_and_a_changed_one_resets_them(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $parallel = $context->throttle(new ScheduledHeartbeat($recording, 1))->stage(StageName::parallel);

        $parallel->reported(
            500,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 120, null, 0.0, null)],
            1000,
            0,
            false,
        );
        $context->seconds(10);
        $parallel->reported(
            500,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 120, null, 0.0, null)],
            1000,
            0,
            false,
        );
        $context->seconds(1);
        $parallel->reported(
            600,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 140, null, 0.0, null)],
            1000,
            0,
            false,
        );

        static::assertSame(
            [10.0, 0.0],
            array_map(static fn(Pulse $pulse): float => $pulse->idleSeconds, $recording->pulses()),
        );
    }

    public function test_a_zero_advance_is_no_progress(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 5));

        $context->seconds(5);
        $throttle->stage(StageName::sort)->advanced(0);

        static::assertSame(5.0, $recording->pulses()[0]->idleSeconds);
    }

    public function test_unread_rows_leave_the_rows_read(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));

        iterator_to_array(
            $throttle->reading(
                (static function (): Generator {
                    yield RowsMother::sequentialIds(8);
                })(),
                $throttle->stage(StageName::read),
            ),
            false,
        );
        $throttle->unread(5);
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame([3, 3], [$throttle->rowsRead(), $recording->pulses()[0]->rowsRead]);
    }

    public function test_a_report_that_changes_only_memory_is_no_progress(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $parallel = $context->throttle(new ScheduledHeartbeat($recording, 1))->stage(StageName::parallel);

        $parallel->reported(
            500,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 120, null, 0.0, null)],
            1000,
            0,
            false,
        );
        $context->seconds(10);
        $parallel->reported(
            500,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 120, null, 0.0, null)],
            2000,
            3000,
            false,
        );

        static::assertSame(10.0, $recording->pulses()[0]->idleSeconds);
    }

    public function test_beats_stay_on_the_interval_grid_when_each_callback_takes_time(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = new HeartbeatThrottle(
            new SteppingClock(),
            new PhpBackend(),
            $context->logger,
            [new ScheduledHeartbeat($recording, 10)],
        );

        foreach (range(1, 40) as $_) {
            $throttle->beatWhenDue();
        }

        static::assertSame([[1, 10.0], [2, 20.0], [3, 30.0], [4, 40.0]], HeartbeatContext::beats($recording));
    }

    public function test_two_objects_of_one_name_are_two_entries(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $first = $throttle->stage(StageName::sort);
        $second = $throttle->stage(StageName::sort);

        $first->advanced(3);
        $second->advanced(5);
        $context->seconds(120);
        $throttle->beatWhenDue();

        $sorts = HeartbeatContext::entries($recording, StageName::sort)[0];
        static::assertSame([3, 5], array_map(static fn(StageProgress $sort): int => $sort->count, $sorts));
        static::assertNotSame($sorts[0]->id, $sorts[1]->id);
    }

    public function test_an_object_built_with_an_id_reports_into_that_entry(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();

        $throttle->stage(StageName::sort, ProgressUnit::rows, 7)->advanced(3);
        $throttle->stage(StageName::sort, ProgressUnit::rows, 7)->advanced(5);
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame(
            [[7, 'sort', ProgressUnit::rows, null, 8, null, 0.0, null]],
            array_map(static fn(StageProgress $stage): array => [
                $stage->id,
                $stage->name->value,
                $stage->unit,
                $stage->subject,
                $stage->count,
                $stage->total,
                $stage->startedAtSeconds,
                $stage->endedAtSeconds,
            ], $recording->pulses()[0]->stages),
        );
    }

    public function test_a_stage_built_outside_a_run_has_no_id(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));

        $throttle->stage(StageName::sort)->advanced(3);
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame(
            [[null, 'sort', ProgressUnit::rows, null, 3, null, 0.0, null]],
            array_map(static fn(StageProgress $stage): array => [
                $stage->id,
                $stage->name->value,
                $stage->unit,
                $stage->subject,
                $stage->count,
                $stage->total,
                $stage->startedAtSeconds,
                $stage->endedAtSeconds,
            ], $recording->pulses()[0]->stages),
        );
    }

    public function test_an_entry_ends_when_its_object_is_done_and_every_list_that_has_it_ended_it(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $sort = $throttle->stage(StageName::sort, ProgressUnit::rows, 7);
        $parallel = $throttle->stage(StageName::parallel);

        $sort->advanced(3);
        $parallel->reported(
            0,
            [new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 4, null, 0.0, null)],
            0,
            0,
            false,
        );
        $sort->done();
        $context->seconds(120);
        $throttle->beatWhenDue();
        $parallel->reported(
            0,
            [new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 4, null, 0.0, 1.0)],
            0,
            0,
            false,
        );
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame(
            [[7, null], [7, 120.0]],
            array_map(
                static fn(array $sorts): array => [$sorts[0]->count, $sorts[0]->endedAtSeconds],
                HeartbeatContext::entries($recording, StageName::sort),
            ),
        );
    }

    public function test_a_call_after_done_opens_the_entry_again(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $sort = $throttle->stage(StageName::sort);

        $sort->advanced(1);
        $sort->done();
        $context->seconds(120);
        $throttle->beatWhenDue();
        $sort->advanced(1);
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame(
            [[1, 0.0], [2, null]],
            array_map(
                static fn(array $sorts): array => [$sorts[0]->count, $sorts[0]->endedAtSeconds],
                HeartbeatContext::entries($recording, StageName::sort),
            ),
        );
    }

    public function test_busy_opens_the_entry_counts_nothing_and_is_progress(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 10));
        $throttle->started();

        $context->seconds(10);
        $throttle->stage(StageName::commit, ProgressUnit::files)->busy();

        $pulse = $recording->pulses()[0];
        static::assertSame(0.0, $pulse->idleSeconds);
        static::assertSame(
            [['commit', ProgressUnit::files, 0, null]],
            array_map(static fn(StageProgress $stage): array => [
                $stage->name->value,
                $stage->unit,
                $stage->count,
                $stage->endedAtSeconds,
            ], $pulse->stages),
        );
    }

    public function test_an_object_that_never_advanced_adds_no_entry(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();

        $throttle->stage(StageName::sort)->done();
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame([], $recording->pulses()[0]->stages);
    }

    public function test_entries_are_ordered_by_id(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $sort = $throttle->stage(StageName::sort);
        $join = $throttle->stage(StageName::join);

        $join->advanced(1);
        $throttle->stage(StageName::parallel)->reported(
            5,
            [
                new StageProgress(99, StageName::group_by, ProgressUnit::groups, null, 2, null, 0.0, null),
                new StageProgress(null, StageName::read, ProgressUnit::rows, null, 5, null, 0.0, null),
            ],
            0,
            0,
            false,
        );
        $sort->advanced(1);
        $join->advanced(1);
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame(
            ['sort', 'join', 'group_by'],
            array_map(static fn(StageProgress $stage): string => $stage->name->value, $recording->pulses()[0]->stages),
        );
    }

    public function test_a_reported_entry_with_an_id_adds_to_that_entry(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();

        $throttle->stage(StageName::sort, ProgressUnit::rows, 7)->advanced(3);
        $throttle->stage(StageName::parallel)->reported(
            0,
            [new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 4, null, 0.0, null)],
            0,
            0,
            false,
        );
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame(
            [[7, 7]],
            array_map(static fn(StageProgress $sort): array => [
                $sort->id,
                $sort->count,
            ], HeartbeatContext::entries($recording, StageName::sort)[0]),
        );
    }

    public function test_two_launches_add_their_latest_lists_by_id(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $waveOne = $throttle->stage(StageName::parallel);
        $waveTwo = $throttle->stage(StageName::parallel);

        $waveOne->reported(
            500,
            [new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 300, null, 0.0, null)],
            1000,
            9_000_000_000,
            false,
        );
        $waveOne->reported(
            500,
            [new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 500, null, 0.0, null)],
            0,
            9_000_000_000,
            false,
        );
        $context->seconds(120);
        $waveTwo->reported(
            0,
            [new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 40, null, 0.0, null)],
            2000,
            8_000_000_000,
            false,
        );

        $pulse = $recording->pulses()[0];
        static::assertSame([500, 9_000_000_000], [$pulse->rowsRead, $pulse->peakMemoryBytes]);
        static::assertSame(
            [[7, 540]],
            array_map(static fn(StageProgress $sort): array => [
                $sort->id,
                $sort->count,
            ], HeartbeatContext::entries($recording, StageName::sort)[0]),
        );
    }

    public function test_a_reported_read_without_an_id_is_dropped_and_its_rows_still_count(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();

        $throttle->stage(StageName::parallel)->reported(
            4,
            [new StageProgress(null, StageName::read, ProgressUnit::rows, null, 4, null, 0.0, null)],
            0,
            0,
            false,
        );
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame([[], 4], [$recording->pulses()[0]->stages, $recording->pulses()[0]->rowsRead]);
    }

    public function test_a_reported_entry_with_no_own_part_takes_the_location_of_its_parts(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();

        $throttle->stage(StageName::parallel)->reported(
            4,
            [new StageProgress(7, StageName::read, ProgressUnit::rows, 'memory://in/orders.floe', 4, 4, 0.0, null)],
            0,
            0,
            false,
        );
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame(
            [['memory://in/orders.floe', null]],
            array_map(static fn(StageProgress $read): array => [
                $read->subject,
                $read->total,
            ], HeartbeatContext::entries($recording, StageName::read)[0]),
        );
    }

    public function test_a_reported_commit_without_an_id_is_dropped(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();

        $throttle->stage(StageName::parallel)->reported(
            0,
            [new StageProgress(null, StageName::commit, ProgressUnit::files, 'memory://out/', 3, null, 0.0, null)],
            0,
            0,
            false,
        );
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame([], $recording->pulses()[0]->stages);
    }

    public function test_a_reported_entry_starts_and_ends_on_this_clock(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $parallel = $throttle->stage(StageName::parallel);

        $context->seconds(5);
        $parallel->reported(
            0,
            [new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 4, null, 100.0, null)],
            0,
            0,
            false,
        );
        $context->seconds(5);
        $parallel->reported(
            0,
            [new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 6, null, 100.0, 200.0)],
            0,
            0,
            false,
        );
        $context->seconds(110);
        $throttle->beatWhenDue();

        static::assertSame(
            [[5.0, 10.0]],
            array_map(static fn(StageProgress $sort): array => [
                $sort->startedAtSeconds,
                $sort->endedAtSeconds,
            ], HeartbeatContext::entries($recording, StageName::sort)[0]),
        );
    }

    public function test_a_reported_total_never_becomes_the_total_of_an_entry(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();

        $throttle->stage(StageName::parallel)->reported(
            0,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 1, 10, 0.0, null)],
            0,
            0,
            false,
        );
        $context->seconds(120);
        $throttle->beatWhenDue();
        $throttle->stage(StageName::parallel)->reported(
            0,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 1, null, 0.0, null)],
            0,
            0,
            false,
        );
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame(
            [null, null],
            array_map(
                static fn(array $joins): ?int => $joins[0]->total,
                HeartbeatContext::entries($recording, StageName::join),
            ),
        );
    }

    public function test_completed_closes_every_entry_and_beats_every_heartbeat(): void
    {
        $context = new HeartbeatContext();
        $first = new RecordingHeartbeat();
        $second = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($first, 120), new ScheduledHeartbeat($second, 60));
        $throttle->started();

        $throttle->stage(StageName::sort)->advanced(1);
        $context->seconds(5);
        $throttle->completed();

        static::assertSame([[[1, 5.0]], [[1, 5.0]]], [
            array_map(
                static fn(array $sorts): array => [$sorts[0]->count, $sorts[0]->endedAtSeconds],
                HeartbeatContext::entries($first, StageName::sort),
            ),
            array_map(
                static fn(array $sorts): array => [$sorts[0]->count, $sorts[0]->endedAtSeconds],
                HeartbeatContext::entries($second, StageName::sort),
            ),
        ]);
    }

    public function test_completed_in_a_nested_run_does_nothing(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $throttle->started();

        $throttle->stage(StageName::sort)->advanced(1);
        $throttle->completed();

        static::assertSame([], $recording->pulses());
    }

    public function test_completed_after_a_stop_does_not_beat(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $recording = new RecordingHeartbeat(throws: $failure);
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $context->seconds(120);

        try {
            $throttle->beatWhenDue();
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        $throttle->completed();

        static::assertCount(1, $recording->pulses());
    }

    public function test_seconds_keep_their_fraction(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $context->clock->set(new DateTimeImmutable('2026-01-01 10:00:00.900000'));
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 1));

        $context->clock->set(new DateTimeImmutable('2026-01-01 10:00:02.150000'));
        $throttle->beatWhenDue();

        static::assertSame(1.25, $recording->pulses()[0]->elapsedSeconds);
    }

    public function test_unread_rows_lower_the_read_count(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();

        iterator_to_array(
            $throttle->reading(
                (static function (): Generator {
                    yield RowsMother::sequentialIds(8);
                })(),
                $throttle->stage(StageName::read),
            ),
            false,
        );
        $throttle->unread(5);
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame([3, [3]], [
            $recording->pulses()[0]->rowsRead,
            array_map(
                static fn(StageProgress $read): int => $read->count,
                HeartbeatContext::entries($recording, StageName::read)[0],
            ),
        ]);
    }

    public function test_a_nested_run_in_a_worker_keeps_the_parent_ids_apart_and_its_counts(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttleToParent(new ScheduledHeartbeat($recording, 120));
        $throttle->stage(StageName::sort, ProgressUnit::rows, 0)->advanced(5);
        iterator_to_array($throttle->reading(HeartbeatContext::batches(1), $throttle->stage(StageName::read)), false);

        $throttle->started();
        $throttle->stage(StageName::infer)->advanced(8);
        $throttle->finished();
        $context->seconds(120);
        $throttle->beatWhenDue();

        $pulse = $recording->pulses()[0];
        static::assertSame(
            [[0, 'sort', 5], [null, 'read', 10], [null, 'infer', 8]],
            array_map(static fn(StageProgress $stage): array => [
                $stage->id,
                $stage->name->value,
                $stage->count,
            ], $pulse->stages),
        );
        static::assertSame(10, $pulse->rowsRead);
    }

    public function test_a_throttle_reporting_to_its_parent_never_beats_at_the_end_of_a_run(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttleToParent(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $throttle->stage(StageName::sort)->advanced(1);

        $throttle->completed();

        static::assertSame([], $recording->pulses());
    }

    public function test_counting_in_another_unit_starts_the_count_again(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $groupBy = $throttle->stage(StageName::group_by);

        $groupBy->advanced(10);
        $groupBy->countsIn(ProgressUnit::groups);
        $groupBy->countsIn(ProgressUnit::groups);
        $groupBy->advanced(3);
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame(
            [[ProgressUnit::groups, 3]],
            array_map(static fn(StageProgress $stage): array => [
                $stage->unit,
                $stage->count,
            ], $recording->pulses()[0]->stages),
        );
    }

    public function test_reported_parts_still_in_rows_add_nothing_to_an_entry_in_groups(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $merging = $throttle->stage(StageName::group_by, ProgressUnit::rows, 7);
        $parallel = $throttle->stage(StageName::parallel);

        $parallel->reported(
            100,
            [new StageProgress(7, StageName::group_by, ProgressUnit::rows, null, 100, null, 0.0, null)],
            0,
            0,
            false,
        );
        $context->seconds(120);
        $throttle->beatWhenDue();
        $merging->countsIn(ProgressUnit::groups);
        $merging->advanced(4);
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame(
            [
                [ProgressUnit::rows,   100],
                [ProgressUnit::groups, 4],
            ],
            array_map(
                static fn(array $entries): array => [$entries[0]->unit, $entries[0]->count],
                HeartbeatContext::entries($recording, StageName::group_by),
            ),
        );
    }

    public function test_only_a_read_has_a_total(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();

        iterator_to_array(
            $throttle->reading(HeartbeatContext::batches(1), $throttle->stage(StageName::read, total: 10)),
            false,
        );
        $throttle->stage(StageName::sort)->advanced(10);
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame(
            [10, null],
            array_map(static fn(StageProgress $stage): ?int => $stage->total, $recording->pulses()[0]->stages),
        );
    }

    public function test_a_reported_part_adds_its_count_and_never_its_total(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();

        $throttle->stage(StageName::read, ProgressUnit::rows, 7, total: 3)->busy();
        $throttle->stage(StageName::parallel)->reported(
            2,
            [new StageProgress(7, StageName::read, ProgressUnit::rows, null, 2, null, 0.0, null)],
            0,
            0,
            false,
        );
        $context->seconds(120);
        $throttle->beatWhenDue();

        static::assertSame(
            [[2, 3]],
            array_map(static fn(StageProgress $read): array => [
                $read->count,
                $read->total,
            ], HeartbeatContext::entries($recording, StageName::read)[0]),
        );
    }

    public function test_its_interval_is_the_smallest_of_its_heartbeats(): void
    {
        static::assertSame(
            0.25,
            (new HeartbeatContext())
                ->throttle(
                    new ScheduledHeartbeat(new RecordingHeartbeat(), 120),
                    new ScheduledHeartbeat(new RecordingHeartbeat(), 0.25),
                    new ScheduledHeartbeat(new RecordingHeartbeat(), 1),
                )
                ->everySeconds(),
        );
    }

    public function test_a_key_first_seen_already_ended_keeps_the_longest_duration_its_lists_report(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $context->seconds(120);

        $throttle->stage(StageName::parallel)->reported(
            0,
            [
                new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 10, null, 2.0, 2.75),
                new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 5, null, 1.0, 1.5),
            ],
            0,
            0,
            false,
        );

        static::assertSame(
            [[119.25, 120.0]],
            array_map(static fn(StageProgress $stage): array => [
                $stage->startedAtSeconds,
                $stage->endedAtSeconds,
            ], HeartbeatContext::entries($recording, StageName::sort)[0]),
        );
    }

    public function test_a_key_first_seen_open_starts_when_the_parent_sees_it(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $launch = $throttle->stage(StageName::parallel);
        $context->seconds(120);

        $launch->reported(
            0,
            [new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 10, null, 2.0, null)],
            0,
            0,
            false,
        );
        $context->seconds(120);
        $launch->reported(
            0,
            [new StageProgress(7, StageName::sort, ProgressUnit::rows, null, 20, null, 2.0, 200.0)],
            0,
            0,
            false,
        );

        static::assertSame(
            [[120.0, 240.0]],
            array_map(static fn(StageProgress $stage): array => [
                $stage->startedAtSeconds,
                $stage->endedAtSeconds,
            ], HeartbeatContext::entries($recording, StageName::sort)[1]),
        );
    }

    public function test_one_launch_reporting_an_ended_key_again_keeps_its_end(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $launch = $throttle->stage(StageName::parallel);
        $context->seconds(120);

        $launch->reported(
            0,
            [new StageProgress(7, StageName::group_by, ProgressUnit::groups, null, 3, null, 0.0, 0.5)],
            0,
            0,
            false,
        );
        $context->seconds(120);
        $launch->reported(
            0,
            [new StageProgress(7, StageName::group_by, ProgressUnit::groups, null, 4, null, 0.0, 0.5)],
            0,
            0,
            false,
        );

        static::assertSame(
            [[119.5, 120.0]],
            array_map(static fn(StageProgress $stage): array => [
                $stage->startedAtSeconds,
                $stage->endedAtSeconds,
            ], HeartbeatContext::entries($recording, StageName::group_by)[1]),
        );
    }

    public function test_a_second_launch_reporting_an_ended_key_moves_its_end(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $context->seconds(120);

        $throttle->stage(StageName::parallel)->reported(
            0,
            [new StageProgress(7, StageName::group_by, ProgressUnit::rows, null, 30, null, 0.0, 0.5)],
            0,
            0,
            false,
        );
        $context->seconds(120);
        $throttle->stage(StageName::parallel)->reported(
            0,
            [new StageProgress(7, StageName::group_by, ProgressUnit::groups, null, 3, null, 0.0, 0.5)],
            0,
            0,
            false,
        );

        static::assertSame(
            [[119.5, 240.0]],
            array_map(static fn(StageProgress $stage): array => [
                $stage->startedAtSeconds,
                $stage->endedAtSeconds,
            ], HeartbeatContext::entries($recording, StageName::group_by)[1]),
        );
    }

    public function test_a_report_that_progressed_with_no_count_change_resets_the_idle_time(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 10));
        $throttle->started();

        $context->seconds(10);
        $throttle->stage(StageName::parallel)->reported(0, [], 0, 0, true);

        static::assertSame(0.0, $recording->pulses()[0]->idleSeconds);
    }

    public function test_a_report_that_did_not_progress_keeps_the_idle_time_growing(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 10));
        $throttle->started();

        $context->seconds(10);
        $throttle->stage(StageName::parallel)->reported(0, [], 0, 0, false);
        $throttle->beatWhenDue();

        static::assertSame(10.0, $recording->pulses()[0]->idleSeconds);
    }

    public function test_busy_is_progress_with_no_entry(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 10));
        $throttle->started();

        $context->seconds(10);
        $throttle->busy();

        static::assertSame([0.0, []], [$recording->pulses()[0]->idleSeconds, $recording->pulses()[0]->stages]);
    }

    public function test_busy_in_a_worker_makes_its_next_report_progress(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttleToParent(new ScheduledHeartbeat($recording, 10));
        $throttle->started();
        $context->seconds(10);
        $throttle->beatWhenDue();

        $context->seconds(10);
        $throttle->busy();

        static::assertSame(
            [10.0, 0.0],
            array_map(static fn(Pulse $pulse): float => $pulse->idleSeconds, $recording->pulses()),
        );
    }

    public function test_a_stopped_throttle_calls_no_heartbeat_when_busy(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $recording = new RecordingHeartbeat(throws: $failure);
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $context->seconds(120);

        try {
            $throttle->busy();
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        $context->seconds(120);
        $throttle->busy();

        static::assertCount(1, $recording->pulses());
    }

    public function test_the_peak_is_the_highest_total_of_parent_and_workers_and_stays_after_they_end(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $first = $throttle->stage(StageName::parallel);
        $second = $throttle->stage(StageName::parallel);

        $first->reported(0, [], 300 * 1_048_576, 300 * 1_048_576, true);
        $second->reported(0, [], 300 * 1_048_576, 300 * 1_048_576, true);
        $context->seconds(120);
        $throttle->beatWhenDue();
        $first->reported(0, [], 0, 300 * 1_048_576, true);
        $second->reported(0, [], 0, 300 * 1_048_576, true);
        $context->seconds(120);
        $throttle->beatWhenDue();

        [$running, $ended] = $recording->pulses();
        static::assertGreaterThanOrEqual(600 * 1_048_576, $running->memoryBytes - $ended->memoryBytes);
        static::assertGreaterThanOrEqual($running->memoryBytes, $running->peakMemoryBytes);
        static::assertSame($running->peakMemoryBytes, $ended->peakMemoryBytes);
    }

    public function test_the_peak_counts_a_total_that_rose_and_fell_between_two_beats(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $first = $throttle->stage(StageName::parallel);
        $second = $throttle->stage(StageName::parallel);

        $first->reported(0, [], 300 * 1_048_576, 300 * 1_048_576, true);
        $second->reported(0, [], 300 * 1_048_576, 300 * 1_048_576, true);
        $first->reported(0, [], 0, 300 * 1_048_576, true);
        $second->reported(0, [], 0, 300 * 1_048_576, true);
        $context->seconds(120);
        $throttle->beatWhenDue();

        $pulse = $recording->pulses()[0];
        static::assertGreaterThanOrEqual($pulse->memoryBytes + (600 * 1_048_576), $pulse->peakMemoryBytes);
    }

    public function test_the_peak_is_never_below_the_memory_of_the_same_pulse(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = new HeartbeatThrottle(
            $context->clock,
            new ReportedMemoryBackend(64 * 1_073_741_824),
            $context->logger,
            [new ScheduledHeartbeat($recording, 120)],
        );
        $throttle->started();
        $context->seconds(120);
        $throttle->beatWhenDue();

        $pulse = $recording->pulses()[0];
        static::assertGreaterThanOrEqual($pulse->memoryBytes, $pulse->peakMemoryBytes);
    }

    public function test_a_serial_run_peak_is_at_least_its_sampled_memory_and_at_most_the_php_peak(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();
        $context->seconds(120);
        $throttle->beatWhenDue();

        $pulse = $recording->pulses()[0];
        static::assertGreaterThanOrEqual($pulse->memoryBytes, $pulse->peakMemoryBytes);
        static::assertLessThanOrEqual(max(memory_get_peak_usage(true), $pulse->memoryBytes), $pulse->peakMemoryBytes);
    }

    public function test_the_read_stage_is_the_planned_one_when_the_plan_has_it(): void
    {
        $throttle = (new HeartbeatContext())->throttle(new ScheduledHeartbeat(new RecordingHeartbeat(), 120));
        $planned = $throttle->stage(StageName::read);

        static::assertSame($planned, $throttle->readStage($planned, from_array([['id' => 1]]), null));
    }

    public function test_a_plan_without_a_read_stage_reads_into_a_new_one_at_the_file_it_reads(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 120));
        $throttle->started();

        $throttle
            ->readStage(
                new VoidProgress(),
                (new RecordingFileExtractor(schema(int_schema('id'))))->withSource(path('memory://in/orders.csv')),
                null,
            )
            ->read(1);
        $context->seconds(120);
        $throttle->beatWhenDue();

        $read = $recording->pulses()[0]->stages[0];
        static::assertSame(['read', 'memory://in/orders.csv'], [$read->name->value, $read->subject]);
    }
}
