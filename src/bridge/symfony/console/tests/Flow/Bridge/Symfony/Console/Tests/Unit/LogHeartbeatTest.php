<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Unit;

use Flow\Bridge\Symfony\Console\LogHeartbeat;
use Flow\Bridge\Symfony\Console\Tests\Context\TerminalContext;
use Flow\Bridge\Symfony\Console\Tests\Mother\PulseMother;
use Flow\Bridge\Symfony\Console\Tests\Mother\StageProgressMother;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Tests\FlowTestCase;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Output\StreamOutput;

use function Flow\Types\DSL\type_instance_of;
use function preg_match_all;
use function str_contains;
use function substr_count;

final class LogHeartbeatTest extends FlowTestCase
{
    public function test_no_progress_line_before_ten_seconds(): void
    {
        $output = new BufferedOutput();

        (new LogHeartbeat(
            $output,
        ))->beat(PulseMother::pulse(elapsedSeconds: 9.9, stages: [StageProgressMother::open()]));

        static::assertSame('', $output->fetch());
    }

    public function test_every_ten_seconds_one_line_per_open_entry(): void
    {
        $output = new BufferedOutput();
        $heartbeat = new LogHeartbeat($output);
        $stages = [
            StageProgressMother::open(StageName::read, 21_541_120, 30_000_000, subject: 'memory://in/orders.csv'),
            StageProgressMother::open(StageName::sort, 11_000_000, id: 2),
        ];

        $heartbeat->beat(PulseMother::pulse(memoryBytes: 633_339_904, elapsedSeconds: 10.0, stages: $stages));
        $heartbeat->beat(PulseMother::pulse(beat: 2, memoryBytes: 633_339_904, elapsedSeconds: 15.0, stages: $stages));

        static::assertSame(
            "[00:10] read      orders.csv  21,541,120 of 30,000,000 rows (71%), 2.2M rows/s, memory 604 MiB\n"
            . "[00:10] sort      11,000,000 rows, 1.1M rows/s, memory 604 MiB\n",
            $output->fetch(),
        );
    }

    public function test_a_done_line_has_its_own_stamp_and_is_printed_once(): void
    {
        $output = new BufferedOutput();
        $heartbeat = new LogHeartbeat($output);
        $stages = [StageProgressMother::done(
            StageName::commit,
            82,
            7.6,
            9.2,
            subject: 'memory://warehouse/orders/',
            unit: ProgressUnit::files,
        )];

        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 12.0, stages: $stages));
        $heartbeat->beat(PulseMother::pulse(beat: 2, elapsedSeconds: 13.0, stages: $stages));

        static::assertSame("[00:09] commit    warehouse/orders/  done 82 files in 1.6s\n", $output->fetch());
    }

    public function test_finish_after_a_completed_run_writes_the_finished_line(): void
    {
        $output = new BufferedOutput();
        $heartbeat = new LogHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(
            rowsRead: 30_000_000,
            peakMemoryBytes: 671_088_640,
            elapsedSeconds: 9.2,
            stages: [
                StageProgressMother::done(StageName::read, 30_000_000, endedAtSeconds: 5.0, id: 2),
                StageProgressMother::done(StageName::commit, 82, 7.6, 9.2, unit: ProgressUnit::files),
            ],
        ));
        $output->fetch();

        $heartbeat->finish();

        static::assertSame(
            "[00:09] finished  30,000,000 rows in 9.2s, peak memory 640 MiB, 82 files written\n",
            $output->fetch(),
        );
    }

    public function test_one_row_one_group_and_one_file_are_singular_in_the_lines_and_the_finished_line(): void
    {
        $output = new BufferedOutput();
        $heartbeat = new LogHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(rowsRead: 1, peakMemoryBytes: 671_088_640, elapsedSeconds: 10.0, stages: [
            StageProgressMother::open(StageName::read, 1, 1),
            StageProgressMother::open(StageName::sort, 1, id: 2),
        ]));
        $heartbeat->beat(PulseMother::pulse(
            beat: 2,
            rowsRead: 1,
            peakMemoryBytes: 671_088_640,
            elapsedSeconds: 11.0,
            stages: [
                StageProgressMother::done(StageName::read, 1, endedAtSeconds: 10.5),
                StageProgressMother::done(StageName::group_by, 1, 10.5, 10.8, id: 2, unit: ProgressUnit::groups),
                StageProgressMother::done(StageName::commit, 1, 10.8, 11.0, id: 3, unit: ProgressUnit::files),
            ],
        ));

        $heartbeat->finish();

        static::assertSame(
            "[00:10] read      1 of 1 row (100%), 0 rows/s, memory 142 MiB\n"
            . "[00:10] sort      1 row, 0 rows/s, memory 142 MiB\n"
            . "[00:10] read      done 1 row in 10.5s, 0 rows/s\n"
            . "[00:10] group by  done 1 group in 0.3s\n"
            . "[00:11] commit    done 1 file in 0.2s\n"
            . "[00:11] finished  1 row in 11.0s, peak memory 640 MiB, 1 file written\n",
            $output->fetch(),
        );
    }

    public function test_no_summary_without_a_read_entry(): void
    {
        $output = new BufferedOutput();
        $heartbeat = new LogHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(rowsRead: 0, elapsedSeconds: 1.0, stages: [
            StageProgressMother::done(StageName::infer, 2_500, endedAtSeconds: 1.0, id: null),
        ]));

        $heartbeat->finish();

        static::assertStringNotContainsString('finished', $output->fetch());
    }

    public function test_a_new_run_starts_its_progress_lines_again(): void
    {
        $output = new BufferedOutput();
        $heartbeat = new LogHeartbeat($output);
        $stages = [StageProgressMother::open(StageName::sort, 11_000_000)];

        $heartbeat->beat(PulseMother::pulse(memoryBytes: 633_339_904, elapsedSeconds: 10.0, stages: $stages));
        $heartbeat->beat(PulseMother::pulse(memoryBytes: 633_339_904, elapsedSeconds: 10.0, stages: $stages));

        static::assertSame(2, substr_count($output->fetch(), '[00:10] sort      11,000,000 rows'));
    }

    public function test_a_planned_entry_prints_nothing_and_a_skipped_one_prints_skipped_once(): void
    {
        $output = new BufferedOutput();
        $heartbeat = new LogHeartbeat($output);

        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 10.0, stages: [StageProgressMother::planned()]));
        $heartbeat->beat(PulseMother::pulse(beat: 2, elapsedSeconds: 12.0, stages: [
            StageProgressMother::skipped(endedAtSeconds: 11.0),
        ]));
        $heartbeat->beat(PulseMother::pulse(beat: 3, elapsedSeconds: 13.0, stages: [
            StageProgressMother::skipped(endedAtSeconds: 11.0),
        ]));

        static::assertSame("[00:11] sort      skipped\n", $output->fetch());
    }

    public function test_finish_after_a_failed_run_writes_nothing(): void
    {
        $output = new BufferedOutput();
        $heartbeat = new LogHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 1.0, stages: [StageProgressMother::open()]));

        $heartbeat->finish();

        static::assertSame('', $output->fetch());
    }

    public function test_a_quiet_output_prints_nothing(): void
    {
        $output = new BufferedOutput(OutputInterface::VERBOSITY_QUIET);
        $heartbeat = new LogHeartbeat($output);

        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 12.0, stages: [StageProgressMother::done()]));
        $heartbeat->finish();

        static::assertSame('', $output->fetch());
    }

    public function test_an_entry_that_ends_again_prints_a_done_line_for_its_new_end(): void
    {
        $output = new BufferedOutput();
        $heartbeat = new LogHeartbeat($output);

        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 8.0, stages: [StageProgressMother::done(
            StageName::sort,
            10,
            0.0,
            6.0,
        )]));
        $heartbeat->beat(PulseMother::pulse(beat: 2, elapsedSeconds: 10.0, stages: [StageProgressMother::open(
            StageName::sort,
            20,
        )]));
        $heartbeat->beat(PulseMother::pulse(beat: 3, elapsedSeconds: 13.0, stages: [StageProgressMother::done(
            StageName::sort,
            30,
            0.0,
            12.0,
        )]));

        $lines = [];
        preg_match_all('/^(\[\d\d:\d\d\] sort      (?:done )?\d+ rows)/m', $output->fetch(), $lines);
        static::assertSame(
            ['[00:06] sort      done 10 rows', '[00:10] sort      20 rows', '[00:12] sort      done 30 rows'],
            $lines[1],
        );
    }

    public function test_two_ended_entries_of_one_name_and_one_end_print_one_done_line_each(): void
    {
        $output = new BufferedOutput();
        $heartbeat = new LogHeartbeat($output);
        $stages = [
            StageProgressMother::done(StageName::sort, 4, endedAtSeconds: 3.0, id: 1),
            StageProgressMother::done(StageName::sort, 2, endedAtSeconds: 3.0, id: 2),
        ];

        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 4.0, stages: $stages));
        $heartbeat->beat(PulseMother::pulse(beat: 2, elapsedSeconds: 5.0, stages: $stages));

        $lines = [];
        preg_match_all('/^\[00:03\] sort      done (\d+) rows/m', $output->fetch(), $lines);
        static::assertSame(['4', '2'], $lines[1]);
    }

    public function test_a_second_finish_writes_no_second_finished_line(): void
    {
        $output = new BufferedOutput();
        $heartbeat = new LogHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 9.2, stages: [
            StageProgressMother::done(StageName::read, 3, endedAtSeconds: 5.0, id: 2),
            StageProgressMother::done(StageName::commit, 82, 7.6, 9.2, unit: ProgressUnit::files),
        ]));

        $heartbeat->finish();
        $heartbeat->finish();

        static::assertSame(1, substr_count($output->fetch(), 'finished'));
    }

    public function test_built_directly_it_draws_on_the_output_it_was_given(): void
    {
        $terminal = new TerminalContext(100);

        try {
            $console = $terminal->console();

            (new LogHeartbeat($console))->beat(PulseMother::pulse(elapsedSeconds: 3.0, stages: [
                StageProgressMother::done(StageName::read, 3, endedAtSeconds: 1.0),
            ]));

            static::assertSame([true, ''], [
                str_contains($terminal->text($console), ' read '),
                $terminal->raw(type_instance_of(StreamOutput::class)->assert($console->getErrorOutput())),
            ]);
        } finally {
            $terminal->restore();
        }
    }
}
