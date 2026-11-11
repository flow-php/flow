<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Unit;

use Flow\Bridge\Symfony\Console\StatusLineHeartbeat;
use Flow\Bridge\Symfony\Console\Tests\Context\TerminalContext;
use Flow\Bridge\Symfony\Console\Tests\Mother\PulseMother;
use Flow\Bridge\Symfony\Console\Tests\Mother\StageProgressMother;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\StageProgress;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Output\StreamOutput;

use function Flow\Types\DSL\type_instance_of;
use function str_contains;
use function str_replace;
use function strlen;
use function substr;
use function substr_count;

final class StatusLineHeartbeatTest extends FlowTestCase
{
    public TerminalContext $terminal;

    /**
     * @var list<StageProgress>
     */
    public array $stages = [];

    protected function setUp(): void
    {
        parent::setUp();
        $this->terminal = new TerminalContext(120);
        $this->stages = [
            StageProgressMother::done(StageName::read, 30_000_000, 0.0, 5.0, subject: 'memory://in/orders.csv'),
            StageProgressMother::open(StageName::sort, 24_600_000, 30_000_000, 0.2, id: 2, subject: 'memory://in/x/'),
            StageProgressMother::open(StageName::group_by, 12, id: 3, unit: ProgressUnit::groups),
            StageProgressMother::open(StageName::join, 1_000_000, id: 4),
        ];
    }

    protected function tearDown(): void
    {
        $this->terminal->restore();
        parent::tearDown();
    }

    #[TestWith([
        120,
        ' ⠋ read ✔ › sort in/x/ 24.6M/30.0M 82% 4.2M/s · group by 12 groups · join 1.0M rows 166.7K/s 6.0s',
    ])]
    #[TestWith([96, ' ⠋ read ✔ › sort 24.6M/30.0M 82% 4.2M/s · group by 12 groups · join 1.0M rows 166.7K/s 6.0s'])]
    #[TestWith([91, ' ⠋ sort 24.6M/30.0M 82% 4.2M/s · group by 12 groups · join 1.0M rows 166.7K/s 6.0s'])]
    #[TestWith([80, ' ⠋ join 1.0M rows 166.7K/s · +2 running 6.0s'])]
    #[TestWith([40, ' ⠋ join 1.0M rows 166.7K/s 6.0s'])]
    #[TestWith([12, ' ⠋ join 1.0'])]
    public function test_the_first_candidate_that_fits_is_drawn(int $columns, string $line): void
    {
        $this->terminal->columns($columns);
        $output = $this->terminal->stream();

        (new StatusLineHeartbeat($output))->beat(PulseMother::pulse(elapsedSeconds: 6.0, stages: $this->stages));

        static::assertSame($line, str_replace("\r", '', $this->terminal->text($output)));
    }

    public function test_finish_after_a_completed_run_keeps_the_summary(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new StatusLineHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(
            rowsRead: 30_000_000,
            peakMemoryBytes: 671_088_640,
            elapsedSeconds: 9.2,
            stages: [
                StageProgressMother::done(StageName::read, 30_000_000, endedAtSeconds: 5.0),
                StageProgressMother::done(StageName::commit, 82, 7.6, 9.2, id: 2, unit: ProgressUnit::files),
            ],
        ));

        $heartbeat->finish();

        static::assertStringEndsWith(' ✔ 30,000,000 rows in 9.2s · peak 640 MiB · 82 files' . "\n", str_replace(
            "\r",
            "\n",
            $this->terminal->text($output),
        ));
    }

    public function test_one_row_and_one_file_are_singular_in_the_summary(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new StatusLineHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(rowsRead: 1, peakMemoryBytes: 671_088_640, elapsedSeconds: 1.0, stages: [
            StageProgressMother::done(StageName::read, 1, endedAtSeconds: 0.5),
            StageProgressMother::done(StageName::commit, 1, 0.5, 1.0, id: 2, unit: ProgressUnit::files),
        ]));

        $heartbeat->finish();

        static::assertStringEndsWith(' ✔ 1 row in 1.0s · peak 640 MiB · 1 file' . "\n", str_replace(
            "\r",
            "\n",
            $this->terminal->text($output),
        ));
    }

    public function test_no_summary_without_a_read_entry(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new StatusLineHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(rowsRead: 0, elapsedSeconds: 1.0, stages: [
            StageProgressMother::done(StageName::infer, 2_500, endedAtSeconds: 1.0, id: null),
        ]));

        $heartbeat->finish();

        static::assertStringNotContainsString('0 rows in', $this->terminal->text($output));
    }

    public function test_a_planned_entry_is_left_out_and_a_skipped_one_is_finished_without_a_mark(): void
    {
        $output = $this->terminal->stream();

        (new StatusLineHeartbeat($output))->beat(PulseMother::pulse(elapsedSeconds: 6.0, stages: [
            StageProgressMother::skipped(StageName::sort, 5.0, id: 1),
            StageProgressMother::open(StageName::read, 10, id: 2),
            StageProgressMother::planned(StageName::commit, id: 3),
        ]));

        static::assertStringEndsWith(' ⠋ sort › read 10 rows 2/s 6.0s', $this->terminal->text($output));
    }

    public function test_finish_after_a_failed_run_clears_the_line(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new StatusLineHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse());

        $heartbeat->finish();

        static::assertStringEndsWith("\x1B[1G\x1B[2K", $this->terminal->raw($output));
    }

    public function test_built_directly_it_draws_on_the_output_it_was_given(): void
    {
        $console = $this->terminal->console();

        (new StatusLineHeartbeat($console))->beat(PulseMother::pulse());

        static::assertSame([true, ''], [
            str_contains($this->terminal->text($console), ' sort 1.3M rows'),
            $this->terminal->raw(type_instance_of(StreamOutput::class)->assert($console->getErrorOutput())),
        ]);
    }

    public function test_a_quiet_output_prints_nothing(): void
    {
        $output = $this->terminal->stream(OutputInterface::VERBOSITY_QUIET);
        $heartbeat = new StatusLineHeartbeat($output);

        $heartbeat->beat(PulseMother::pulse());
        $heartbeat->finish();

        static::assertSame('', $this->terminal->raw($output));
    }

    public function test_memory_shows_at_verbose(): void
    {
        $output = $this->terminal->stream(OutputInterface::VERBOSITY_VERBOSE);

        (new StatusLineHeartbeat($output))->beat(PulseMother::pulse());

        static::assertStringEndsWith(' sort 1.3M rows 15.1K/s 142 MiB · 1m 23s', $this->terminal->text($output));
    }

    public function test_a_section_is_overwritten_in_place_and_not_cut(): void
    {
        $output = $this->terminal->section();
        $this->terminal->columns(12);
        $heartbeat = new StatusLineHeartbeat($output);

        $heartbeat->beat(PulseMother::pulse(beat: 1));
        $heartbeat->beat(PulseMother::pulse(beat: 2, count: 1_500_000));

        static::assertStringEndsWith(
            " \x1B[36m⠋\x1B[39m \x1B[1msort\x1B[22m 1.5M rows \x1B[33m18.1K/s\x1B[39m \x1B[90m1m 23s\x1B[39m\n",
            $output->getContent(),
        );
        static::assertSame(1, substr_count($output->getContent(), "\n"));
    }

    public function test_finish_clears_a_section(): void
    {
        $output = $this->terminal->section();
        $heartbeat = new StatusLineHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse());

        $heartbeat->finish();

        static::assertSame('', $output->getContent());
    }

    public function test_a_subject_with_console_tags_prints_them_literally(): void
    {
        $output = $this->terminal->stream();

        (new StatusLineHeartbeat($output))->beat(PulseMother::pulse(stages: [
            StageProgressMother::open(count: 1_250_000, subject: '<error>x'),
        ]));

        static::assertStringEndsWith(' sort <error>x 1.3M rows 15.1K/s 1m 23s', $this->terminal->text($output));
    }

    public function test_a_beat_after_finish_starts_a_new_indicator(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new StatusLineHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(beat: 1));
        $heartbeat->finish();
        $finished = strlen($this->terminal->raw($output));

        $heartbeat->beat(PulseMother::pulse(beat: 2, stage: StageName::join));

        static::assertStringEndsWith(' join 1.3M rows 15.1K/s 1m 23s', $this->terminal->text($output));
        static::assertStringNotContainsString(' sort ', substr($this->terminal->raw($output), $finished));
    }

    public function test_finish_without_a_beat_does_nothing(): void
    {
        $output = $this->terminal->stream();
        $output->write('command line');

        (new StatusLineHeartbeat($output))->finish();

        static::assertSame('command line', $this->terminal->raw($output));
    }

    public function test_a_second_finish_leaves_the_output_after_the_first_alone(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new StatusLineHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse());
        $heartbeat->finish();

        $output->write('command line');
        $heartbeat->finish();

        static::assertStringEndsWith("\x1B[1G\x1B[2Kcommand line", $this->terminal->raw($output));
    }

    public function test_a_line_is_at_most_100_columns_on_a_wider_terminal(): void
    {
        $this->terminal->columns(150);
        $output = $this->terminal->stream();

        (new StatusLineHeartbeat($output))->beat(PulseMother::pulse(elapsedSeconds: 2.0, stages: [
            StageProgressMother::open(StageName::sort, 15_301_437, subject: 'memory://in/orders.floe'),
            StageProgressMother::open(StageName::join, 11, id: 2),
            StageProgressMother::open(StageName::group_by, 3, id: 3),
            StageProgressMother::open(StageName::window, 4_000_000, id: 4),
            StageProgressMother::open(StageName::pivot, 5_000_000, id: 5),
        ]));

        static::assertStringEndsWith(' pivot 5.0M rows 2.5M/s · +4 running 2.0s', $this->terminal->text($output));
    }

    public function test_a_terminal_of_a_few_columns_still_gets_one_character(): void
    {
        $this->terminal->columns(2);
        $output = $this->terminal->stream();

        (new StatusLineHeartbeat(
            $output,
        ))->beat(PulseMother::pulse(elapsedSeconds: 2.0, stages: [StageProgressMother::open(
            StageName::sort,
            15_301_437,
        )]));

        static::assertStringEndsWith('⠋ s', $this->terminal->text($output));
    }

    public function test_a_pulse_with_no_stage_shows_the_time_only(): void
    {
        $output = $this->terminal->stream();

        (new StatusLineHeartbeat($output))->beat(PulseMother::pulse(elapsedSeconds: 83.0, stages: []));

        static::assertSame("\r ⠋ 1m 23s", $this->terminal->text($output));
    }
}
