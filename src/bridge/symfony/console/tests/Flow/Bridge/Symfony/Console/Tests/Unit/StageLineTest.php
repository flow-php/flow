<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Unit;

use Flow\Bridge\Symfony\Console\StageLine;
use Flow\Bridge\Symfony\Console\Tests\Mother\PulseMother;
use Flow\Bridge\Symfony\Console\Tests\Mother\StageProgressMother;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function str_repeat;

final class StageLineTest extends FlowTestCase
{
    #[TestWith(['memory://in/orders.csv', 'orders.csv'])]
    #[TestWith(['aws-s3://bucket/warehouse/orders/', 'warehouse/orders/'])]
    #[TestWith(['file:///data/in/*.csv', 'in/*.csv'])]
    #[TestWith(['file:///data/in/part-{part}.floe', 'in/part-{part}.floe'])]
    public function test_location_keeps_the_last_segments(string $location, string $shown): void
    {
        static::assertSame(
            $shown,
            (new StageLine(
                StageProgressMother::open(subject: $location),
                PulseMother::pulse(elapsedSeconds: 2.0),
            ))->subject(24),
        );
    }

    public function test_a_long_location_is_cut_from_the_left(): void
    {
        static::assertSame(
            '…' . str_repeat('a', 15) . '.csv',
            (new StageLine(
                StageProgressMother::open(subject: 'memory://in/' . str_repeat('a', 36) . '.csv'),
                PulseMother::pulse(elapsedSeconds: 2.0),
            ))->subject(20),
        );
    }

    public function test_no_location_below_eight_columns_or_without_one(): void
    {
        static::assertNull((new StageLine(
            StageProgressMother::open(subject: 'memory://in/orders.csv'),
            PulseMother::pulse(elapsedSeconds: 2.0),
        ))->subject(7));
        static::assertNull((new StageLine(
            StageProgressMother::open(),
            PulseMother::pulse(elapsedSeconds: 2.0),
        ))->subject(24));
    }

    public function test_the_label_reads_without_an_underscore(): void
    {
        static::assertSame(
            'group by',
            (new StageLine(
                StageProgressMother::open(StageName::group_by),
                PulseMother::pulse(elapsedSeconds: 2.0),
            ))->label(),
        );
    }

    public function test_an_open_entry_with_a_total_counts_against_it_and_has_a_percentage(): void
    {
        $line = new StageLine(
            StageProgressMother::open(StageName::read, 15_301_437, 30_000_000),
            PulseMother::pulse(elapsedSeconds: 2.0),
        );

        static::assertSame(['15,301,437 / 30,000,000 rows', '15.3M/30.0M', 51], [
            $line->exactCount(),
            $line->shortCount(),
            $line->percent(),
        ]);
    }

    #[TestWith([ProgressUnit::rows, '1 row'])]
    #[TestWith([ProgressUnit::groups, '1 group'])]
    #[TestWith([ProgressUnit::files, '1 file'])]
    public function test_a_count_of_one_names_its_unit_in_the_singular(ProgressUnit $unit, string $count): void
    {
        $line = new StageLine(
            StageProgressMother::open(count: 1, unit: $unit),
            PulseMother::pulse(elapsedSeconds: 2.0),
        );

        static::assertSame([$count, $count], [$line->exactCount(), $line->shortCount()]);
    }

    public function test_a_count_against_a_total_of_one_names_its_unit_in_the_singular(): void
    {
        static::assertSame(['0 / 1 row', '1 / 5 rows'], [
            (new StageLine(
                StageProgressMother::open(StageName::read, 0, 1),
                PulseMother::pulse(elapsedSeconds: 2.0),
            ))->exactCount(),
            (new StageLine(
                StageProgressMother::open(StageName::read, 1, 5),
                PulseMother::pulse(elapsedSeconds: 2.0),
            ))->exactCount(),
        ]);
    }

    public function test_an_entry_without_a_total_or_ended_shows_its_count_only(): void
    {
        $open = new StageLine(StageProgressMother::open(count: 11_300_000), PulseMother::pulse(elapsedSeconds: 2.0));
        $done = new StageLine(
            StageProgressMother::done(StageName::read, 30_000_000),
            PulseMother::pulse(elapsedSeconds: 2.0),
        );

        static::assertSame(['11,300,000 rows', '11.3M rows', '30,000,000 rows', null], [
            $open->exactCount(),
            $open->shortCount(),
            $done->exactCount(),
            $done->percent(),
        ]);
    }

    public function test_rate_is_for_rows_only(): void
    {
        $pulse = PulseMother::pulse(elapsedSeconds: 2.0);

        static::assertSame(['5.5M/s', null, null], [
            (new StageLine(StageProgressMother::open(count: 11_000_000), $pulse))->rate(),
            (new StageLine(
                StageProgressMother::open(StageName::group_by, 40, unit: ProgressUnit::groups),
                $pulse,
            ))->rate(),
            (new StageLine(StageProgressMother::open(StageName::commit, 8, unit: ProgressUnit::files), $pulse))->rate(),
        ]);
    }

    public function test_no_rate_for_a_stage_shorter_than_half_a_second(): void
    {
        static::assertSame([null, null, '1.0M/s'], [
            (new StageLine(StageProgressMother::done(count: 400, endedAtSeconds: 0.4), PulseMother::pulse()))->rate(),
            (new StageLine(
                StageProgressMother::open(count: 400),
                PulseMother::pulse(elapsedSeconds: 0.49),
            ))->perSecond(),
            (new StageLine(
                StageProgressMother::done(count: 500_000, endedAtSeconds: 0.5),
                PulseMother::pulse(),
            ))->rate(),
        ]);
    }

    public function test_duration_runs_to_the_end_or_to_now(): void
    {
        $pulse = PulseMother::pulse(elapsedSeconds: 9.2);

        static::assertSame(['6.6s', '8.0s'], [
            (new StageLine(StageProgressMother::done(startedAtSeconds: 0.2, endedAtSeconds: 6.8), $pulse))->duration(),
            (new StageLine(StageProgressMother::open(startedAtSeconds: 1.2), $pulse))->duration(),
        ]);
    }

    #[TestWith([9.24, '9.2s'])]
    #[TestWith([83.0, '1m 23s'])]
    #[TestWith([3_725.0, '1h 2m'])]
    public function test_seconds_show_tenths_then_minutes_then_hours(float $seconds, string $shown): void
    {
        static::assertSame($shown, StageLine::seconds($seconds));
    }

    #[TestWith([999.0, '999'])]
    #[TestWith([15_300.0, '15.3K'])]
    #[TestWith([15_301_437.0, '15.3M'])]
    public function test_counts_abbreviate_from_ten_thousand(float $value, string $shown): void
    {
        static::assertSame($shown, StageLine::human($value));
    }

    public function test_memory_is_whole_mebibytes(): void
    {
        static::assertSame('640 MiB', StageLine::memory(671_088_640));
    }

    public function test_a_run_read_and_completed_when_it_has_a_read_entry_and_none_is_open(): void
    {
        static::assertSame([true, false, false, false], [
            StageLine::readCompleted(PulseMother::pulse(stages: [
                StageProgressMother::done(StageName::read),
                StageProgressMother::done(id: 2),
            ])),
            StageLine::readCompleted(PulseMother::pulse(stages: [
                StageProgressMother::done(StageName::read),
                StageProgressMother::open(id: 2),
            ])),
            StageLine::readCompleted(PulseMother::pulse(stages: [])),
            StageLine::readCompleted(PulseMother::pulse(stages: [
                StageProgressMother::done(StageName::infer, 2_500, endedAtSeconds: 1.0, id: null),
            ])),
        ]);
    }

    public function test_files_sum_every_commit_entry(): void
    {
        static::assertSame([82, null], [
            StageLine::filesWritten(PulseMother::pulse(stages: [
                StageProgressMother::done(StageName::commit, 40, unit: ProgressUnit::files),
                StageProgressMother::done(StageName::commit, 42, id: 2, unit: ProgressUnit::files),
            ])),
            StageLine::filesWritten(PulseMother::pulse(stages: [StageProgressMother::done()])),
        ]);
    }

    public function test_an_ended_entry_or_a_zero_total_shows_no_percentage(): void
    {
        static::assertSame([null, null], [
            (new StageLine(
                StageProgressMother::done(StageName::read, 30_000_000, total: 30_000_000),
                PulseMother::pulse(elapsedSeconds: 7.0),
            ))->percent(),
            (new StageLine(
                StageProgressMother::open(StageName::read, 0, 0),
                PulseMother::pulse(elapsedSeconds: 2.0),
            ))->percent(),
        ]);
    }
}
