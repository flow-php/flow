<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Heartbeat;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\StageProgress;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\Runtime\Heartbeat\HeartbeatStage;
use Flow\ETL\Runtime\Heartbeat\ScheduledHeartbeat;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Double\RecordingHeartbeat;
use Flow\ETL\Tests\FlowTestCase;
use RuntimeException;

use function array_map;

final class HeartbeatStageTest extends FlowTestCase
{
    public function test_accumulates_its_own_rows(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $stage = new HeartbeatStage(
            StageName::sort,
            ProgressUnit::rows,
            null,
            null,
            $context->throttle(new ScheduledHeartbeat($recording, 1)),
            0,
        );

        $stage->advanced(5);
        $context->seconds(1);
        $stage->advanced(7);

        static::assertSame(
            [['sort', 12]],
            array_map(static fn(StageProgress $sort): array => [
                $sort->name->value,
                $sort->count,
            ], $recording->pulses()[0]->stages),
        );
    }

    public function test_forwards_reported_totals_unchanged(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $stage = new HeartbeatStage(
            StageName::parallel,
            ProgressUnit::rows,
            null,
            null,
            $context->throttle(new ScheduledHeartbeat($recording, 1)),
            0,
        );

        $context->seconds(1);
        $stage->reported(
            500,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 120, null, 0.0, null)],
            1000,
            9_000_000_000,
            false,
        );

        $pulse = $recording->pulses()[0];
        static::assertSame([500, 'join', 120, 9_000_000_000], [
            $pulse->rowsRead,
            $pulse->stages[0]->name->value,
            $pulse->stages[0]->count,
            $pulse->peakMemoryBytes,
        ]);
    }

    public function test_a_report_replaces_its_own_previous_report(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $stage = new HeartbeatStage(
            StageName::parallel,
            ProgressUnit::rows,
            null,
            null,
            $context->throttle(new ScheduledHeartbeat($recording, 1)),
            0,
        );

        $stage->reported(
            500,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 120, null, 0.0, null)],
            1_000_000_000_000,
            0,
            false,
        );
        $context->seconds(1);
        $stage->reported(
            700,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 150, null, 0.0, null)],
            7,
            0,
            false,
        );

        $pulse = $recording->pulses()[0];
        static::assertSame([700, 150], [$pulse->rowsRead, $pulse->stages[0]->count]);
        static::assertLessThan(1_000_000_000_000, $pulse->memoryBytes);
    }

    public function test_ended_neither_beats_nor_throws_and_the_next_due_check_does(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $recording = new RecordingHeartbeat(throws: $failure);
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 1));
        $context->seconds(1);

        (new HeartbeatStage(StageName::parallel, ProgressUnit::rows, null, null, $throttle, 0))->ended(
            500,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 120, null, 0.0, null)],
            1000,
            0,
        );

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
        $stage = new HeartbeatStage(StageName::parallel, ProgressUnit::rows, null, null, $throttle, 0);

        $stage->reported(
            300,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 100, null, 0.0, null)],
            1000,
            0,
            false,
        );
        $stage->ended(
            500,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 120, null, 0.0, null)],
            1000,
            0,
        );
        $context->seconds(1);
        $throttle->beatWhenDue();

        $pulse = $recording->pulses()[0];
        static::assertSame([500, 'join', 120], [
            $pulse->rowsRead,
            $pulse->stages[0]->name->value,
            $pulse->stages[0]->count,
        ]);
    }

    public function test_reported_still_beats_when_due(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $stage = new HeartbeatStage(
            StageName::parallel,
            ProgressUnit::rows,
            null,
            null,
            $context->throttle(new ScheduledHeartbeat($recording, 1)),
            0,
        );
        $context->seconds(1);

        $stage->reported(
            500,
            [new StageProgress(7, StageName::join, ProgressUnit::rows, null, 120, null, 0.0, null)],
            1000,
            0,
            false,
        );

        static::assertCount(1, $recording->pulses());
    }

    public function test_done_without_a_call_before_does_nothing(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 1));

        (new HeartbeatStage(StageName::sort, ProgressUnit::rows, 7, null, $throttle, 0))->done();
        $context->seconds(1);
        $throttle->beatWhenDue();

        static::assertSame([], $recording->pulses()[0]->stages);
    }

    public function test_a_second_done_does_nothing(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $throttle = $context->throttle(new ScheduledHeartbeat($recording, 1));
        $first = new HeartbeatStage(StageName::sort, ProgressUnit::rows, 7, null, $throttle, 0);
        $second = new HeartbeatStage(StageName::sort, ProgressUnit::rows, 7, null, $throttle, 1);

        $first->advanced(1);
        $second->advanced(1);
        $first->done();
        $first->done();
        $context->seconds(1);
        $throttle->beatWhenDue();

        static::assertNull($recording->pulses()[0]->stages[0]->endedAtSeconds);
    }

    public function test_describes_its_name_id_and_location(): void
    {
        $throttle = (new HeartbeatContext())->throttle(new ScheduledHeartbeat(new RecordingHeartbeat(), 1));

        static::assertEquals(
            Description::compose(HeartbeatStage::class, [
                'name' => 'group_by',
                'id' => 7,
                'subject' => 'memory://in/',
            ]),
            (new HeartbeatStage(
                StageName::group_by,
                ProgressUnit::groups,
                7,
                'memory://in/',
                $throttle,
                3,
            ))->describe(),
        );
        static::assertEquals(
            Description::compose(VoidProgress::class, []),
            (new HeartbeatStage(StageName::sort, ProgressUnit::rows, null, null, $throttle, 4))->describe(),
        );
    }

    public function test_from_description_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(HeartbeatStage::class . ' is rebuilt by Rebuild::progress()');

        HeartbeatStage::fromDescription([], new Rebuild());
    }

    public function test_reports_every_as_often_as_its_most_frequent_heartbeat(): void
    {
        static::assertSame(
            0.25,
            (new HeartbeatContext())
                ->throttle(
                    new ScheduledHeartbeat(new RecordingHeartbeat(), 120),
                    new ScheduledHeartbeat(new RecordingHeartbeat(), 0.25),
                )
                ->stage(StageName::sort)
                ->reportsEverySeconds(),
        );
    }
}
