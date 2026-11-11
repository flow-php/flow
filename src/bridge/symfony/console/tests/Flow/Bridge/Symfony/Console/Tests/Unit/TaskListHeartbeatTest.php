<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Unit;

use Flow\Bridge\Symfony\Console\ConsoleHeartbeatFactory;
use Flow\Bridge\Symfony\Console\ConsoleStyle;
use Flow\Bridge\Symfony\Console\TaskListHeartbeat;
use Flow\Bridge\Symfony\Console\Tests\Context\TerminalContext;
use Flow\Bridge\Symfony\Console\Tests\Mother\PulseMother;
use Flow\Bridge\Symfony\Console\Tests\Mother\StageProgressMother;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;
use Symfony\Component\Console\Helper\Helper;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Output\StreamOutput;

use function Flow\Types\DSL\type_instance_of;
use function preg_match_all;
use function rtrim;
use function str_contains;
use function strlen;
use function strrpos;
use function substr;
use function substr_count;

final class TaskListHeartbeatTest extends FlowTestCase
{
    public TerminalContext $terminal;

    protected function setUp(): void
    {
        parent::setUp();
        $this->terminal = new TerminalContext(100);
    }

    protected function tearDown(): void
    {
        $this->terminal->restore();
        parent::tearDown();
    }

    public function test_open_entries_at_100_columns(): void
    {
        $output = $this->terminal->stream();

        (new TaskListHeartbeat($output))->beat(PulseMother::pulse(elapsedSeconds: 2.0, stages: [
            StageProgressMother::open('read', 15_301_437, 30_000_000, location: 'memory://in/orders.csv'),
            StageProgressMother::open('sort', 11_300_000, id: 2, startedAtSeconds: 0.2),
        ]));

        static::assertSame(
            "  ⠙ read      orders.csv  15,301,437 / 30,000,000 rows  51% ......................... 7.7M/s · 2.0s\n"
            . "  ⠙ sort      11,300,000 rows ....................................................... 6.3M/s · 1.8s\n",
            $this->terminal->text($output),
        );
    }

    #[TestWith([60, '  ⠙ read      orders.csv  15.3M/30.0M  51% ........... 2.0s'])]
    #[TestWith([51, '  ⠙ read      …ders.csv  15.3M/30.0M  51% ... 2.0s'])]
    #[TestWith([48, '  ⠙ read      15.3M/30.0M  51% ........... 2.0s'])]
    public function test_a_narrow_line_drops_the_rate_shortens_the_count_then_cuts_and_drops_the_location(
        int $columns,
        string $line,
    ): void {
        $this->terminal->columns($columns);
        $output = $this->terminal->stream();

        (new TaskListHeartbeat($output))->beat(PulseMother::pulse(elapsedSeconds: 2.0, stages: [
            StageProgressMother::open('read', 15_301_437, 30_000_000, location: 'memory://in/orders.csv'),
        ]));

        static::assertSame($line . "\n", $this->terminal->text($output));
    }

    public function test_memory_shows_at_verbose(): void
    {
        $output = $this->terminal->stream(OutputInterface::VERBOSITY_VERBOSE);

        (new TaskListHeartbeat($output))->beat(PulseMother::pulse(elapsedSeconds: 2.0, stages: [
            StageProgressMother::open('sort', 11_300_000),
        ]));

        static::assertSame(
            "  ⠙ sort      11,300,000 rows ............................................. 5.7M/s · 142 MiB · 2.0s\n",
            $this->terminal->text($output),
        );
    }

    public function test_two_entries_of_one_name_are_two_lines_and_two_done_lines(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);

        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 2.0, stages: [
            StageProgressMother::open('sort', 4, id: 1),
            StageProgressMother::open('sort', 2, id: 2),
        ]));
        $heartbeat->beat(PulseMother::pulse(beat: 2, elapsedSeconds: 3.0, stages: [
            StageProgressMother::done('sort', 6, endedAtSeconds: 2.5, id: 1),
            StageProgressMother::done('sort', 6, endedAtSeconds: 3.0, id: 2),
        ]));

        $text = $this->terminal->text($output);
        static::assertSame(2, substr_count($text, ' sort      4 rows ') + substr_count($text, ' sort      2 rows '));
        static::assertSame(2, substr_count($text, ' DONE'));
    }

    public function test_a_done_line_keeps_its_text_on_every_redraw(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);
        $stages = [
            StageProgressMother::done('read', 30, location: 'memory://in/orders.csv'),
            StageProgressMother::open(id: 2),
        ];

        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 8.0, stages: $stages));
        $heartbeat->beat(PulseMother::pulse(beat: 2, elapsedSeconds: 9.0, stages: $stages));

        $done = [];
        preg_match_all('/^  ✔ read      orders\.csv  30 rows .*DONE$/mu', $this->terminal->text($output), $done);
        static::assertCount(2, $done[0]);
        static::assertSame($done[0][0], $done[0][1]);
    }

    public function test_finish_after_a_completed_run_prints_the_summary_with_every_commit(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(
            rowsRead: 30_000_000,
            peakMemoryBytes: 671_088_640,
            elapsedSeconds: 9.2,
            stages: [
                StageProgressMother::done('read', 30_000_000, endedAtSeconds: 5.0),
                StageProgressMother::done(
                    'commit',
                    40,
                    7.6,
                    9.2,
                    id: 2,
                    location: 'memory://a/',
                    unit: ProgressUnit::files,
                ),
                StageProgressMother::done(
                    'commit',
                    42,
                    7.6,
                    9.2,
                    id: 3,
                    location: 'memory://b/',
                    unit: ProgressUnit::files,
                ),
            ],
        ));

        $heartbeat->finish();

        static::assertStringEndsWith(
            "\n   DONE  30,000,000 rows in 9.2s · peak 640 MiB · 82 files written\n\n",
            $this->terminal->text($output),
        );
    }

    public function test_the_summary_names_files_only_with_a_commit_entry(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(rowsRead: 3, peakMemoryBytes: 671_088_640, elapsedSeconds: 1.0, stages: [
            StageProgressMother::done('read', 3, endedAtSeconds: 1.0),
        ]));

        $heartbeat->finish();

        static::assertStringEndsWith("\n   DONE  3 rows in 1.0s · peak 640 MiB\n\n", $this->terminal->text($output));
    }

    public function test_finish_after_a_failed_run_only_clears_the_open_lines(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(stages: [StageProgressMother::open()]));

        $heartbeat->finish();

        static::assertStringNotContainsString(' DONE ', $this->terminal->text($output));
        static::assertStringEndsWith("\x1B[1A\x1B[0J", $this->terminal->raw($output));
    }

    public function test_a_beat_after_finish_starts_a_new_block(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(stages: [StageProgressMother::done('read', 3, endedAtSeconds: 1.0)]));
        $heartbeat->finish();
        $before = $this->terminal->text($output);

        $heartbeat->beat(PulseMother::pulse(stages: [StageProgressMother::done('read', 3, endedAtSeconds: 1.0)]));

        static::assertStringStartsWith($before . '  ✔ read      3 rows ', $this->terminal->text($output));
    }

    public function test_an_entry_without_a_location_has_no_location_column(): void
    {
        $output = $this->terminal->stream();

        (new TaskListHeartbeat($output))->beat(PulseMother::pulse(stages: [StageProgressMother::done(
            'group_by',
            40,
            6.6,
            7.6,
            unit: ProgressUnit::groups,
        )]));

        static::assertSame(
            "  ✔ group by  40 groups ................................................................. 1.0s DONE\n",
            $this->terminal->text($output),
        );
    }

    public function test_a_quiet_output_prints_nothing(): void
    {
        $output = $this->terminal->stream(OutputInterface::VERBOSITY_QUIET);
        $heartbeat = new TaskListHeartbeat($output);

        $heartbeat->beat(PulseMother::pulse(stages: [StageProgressMother::done('read', 3, endedAtSeconds: 1.0)]));
        $heartbeat->finish();

        static::assertSame('', $this->terminal->raw($output));
    }

    public function test_done_lines_stand_in_the_order_of_the_list_not_in_the_order_the_stages_ended(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(stages: [
            StageProgressMother::open('read', 3, location: 'memory://in/orders.floe'),
            StageProgressMother::done('read', 2, endedAtSeconds: 1.0, id: 2, location: 'memory://in/customers.floe'),
        ]));

        $heartbeat->beat(PulseMother::pulse(beat: 2, stages: [
            StageProgressMother::done('read', 3, endedAtSeconds: 2.0, location: 'memory://in/orders.floe'),
            StageProgressMother::done('read', 2, endedAtSeconds: 1.0, id: 2, location: 'memory://in/customers.floe'),
        ]));

        $text = $this->terminal->text($output);
        static::assertLessThan((int) strrpos($text, 'customers.floe'), (int) strrpos($text, 'orders.floe  3 rows'));
    }

    public function test_a_line_turns_into_its_done_form_in_place(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(stages: [
            StageProgressMother::open('read', 3, location: 'memory://in/orders.floe'),
            StageProgressMother::open('read', 2, id: 2, location: 'memory://in/customers.floe'),
        ]));

        $heartbeat->beat(PulseMother::pulse(beat: 2, stages: [
            StageProgressMother::open('read', 3, location: 'memory://in/orders.floe'),
            StageProgressMother::done('read', 2, endedAtSeconds: 1.0, id: 2, location: 'memory://in/customers.floe'),
        ]));

        $redraw = substr($this->terminal->raw($output), (int) strrpos($this->terminal->raw($output), "\x1B[2A\x1B[0J"));
        static::assertMatchesRegularExpression('/orders\.floe.*\n.*✔.*customers\.floe/u', $redraw);
    }

    public function test_done_lines_above_the_screen_stay_as_printed_and_the_rest_is_redrawn(): void
    {
        $this->terminal->lines(4);
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);
        $stages = [
            StageProgressMother::done('read', 1, id: 1),
            StageProgressMother::done('sort', 1, id: 2),
            StageProgressMother::done('join', 1, id: 3),
            StageProgressMother::done('window', 1, id: 4),
            StageProgressMother::open('group_by', 1, id: 5),
        ];

        $heartbeat->beat(PulseMother::pulse(stages: $stages));
        $heartbeat->beat(PulseMother::pulse(beat: 2, stages: $stages));
        $heartbeat->beat(PulseMother::pulse(beat: 3, stages: $stages));

        $ups = [];
        preg_match_all('/\x1B\[(\d+)A/', $this->terminal->raw($output), $ups);
        static::assertSame(['3', '3'], $ups[1], 'a terminal of 4 lines reaches 3 lines up');
        static::assertSame([1, 3], [
            substr_count($this->terminal->text($output), ' read '),
            substr_count($this->terminal->text($output), ' group by '),
        ]);
    }

    public function test_finish_keeps_the_done_lines_in_the_order_of_the_list(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(stages: [
            StageProgressMother::open('read', 3, location: 'memory://in/orders.floe'),
            StageProgressMother::done('read', 2, endedAtSeconds: 1.0, id: 2, location: 'memory://in/customers.floe'),
        ]));
        $heartbeat->beat(PulseMother::pulse(beat: 2, stages: [
            StageProgressMother::done('read', 3, endedAtSeconds: 2.0, location: 'memory://in/orders.floe'),
            StageProgressMother::done('read', 2, endedAtSeconds: 1.0, id: 2, location: 'memory://in/customers.floe'),
        ]));

        $heartbeat->finish();

        $text = $this->terminal->text($output);
        static::assertLessThan((int) strrpos($text, 'customers.floe'), (int) strrpos($text, 'orders.floe  3 rows'));
    }

    public function test_an_entry_that_opens_again_draws_live_and_ends_with_the_numbers_of_its_new_end(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);

        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 8.0, stages: [StageProgressMother::done(
            'sort',
            10,
            0.0,
            6.0,
        )]));
        $heartbeat->beat(PulseMother::pulse(beat: 2, elapsedSeconds: 9.0, stages: [StageProgressMother::open(
            'sort',
            20,
        )]));
        $heartbeat->beat(PulseMother::pulse(beat: 3, elapsedSeconds: 13.0, stages: [StageProgressMother::done(
            'sort',
            30,
            0.0,
            12.0,
        )]));
        $heartbeat->finish();

        $lines = [];
        preg_match_all('/^  (\S) sort      (\d+) rows .*$/mu', $this->terminal->text($output), $lines);
        static::assertSame(['✔', '⠹', '✔', '✔'], $lines[1]);
        static::assertSame(['10', '20', '30', '30'], $lines[2]);
        static::assertStringEndsWith('12.0s DONE', $lines[0][3]);
    }

    public function test_two_ended_entries_of_one_name_and_one_end_keep_their_own_lines(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);

        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 4.0, stages: [
            StageProgressMother::done('sort', 4, endedAtSeconds: 3.0, id: 1),
            StageProgressMother::done('sort', 2, endedAtSeconds: 3.0, id: 2),
        ]));
        $heartbeat->finish();

        static::assertSame([2, 2], [
            substr_count($this->terminal->text($output), ' sort      4 rows '),
            substr_count($this->terminal->text($output), ' sort      2 rows '),
        ]);
    }

    #[TestWith([89, false])]
    #[TestWith([90, true])]
    #[TestWith([91, true])]
    public function test_the_graph_style_draws_a_sparkline_from_90_columns(int $columns, bool $drawn): void
    {
        $this->terminal->columns($columns);
        $output = $this->terminal->stream();

        (new ConsoleHeartbeatFactory())
            ->create($output, ConsoleStyle::graph)
            ->beat(PulseMother::pulse(elapsedSeconds: 2.0, stages: [StageProgressMother::open(count: 11_300_000)]));

        static::assertSame($drawn, str_contains($this->terminal->text($output), '█'));
    }

    public function test_a_beat_after_finish_never_moves_the_cursor_into_the_finished_block(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);
        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 2.0, stages: [StageProgressMother::done(
            endedAtSeconds: 2.0,
        )]));
        $heartbeat->finish();
        $finished = strlen($this->terminal->raw($output));

        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 3.0, stages: [StageProgressMother::open(id: 2)]));

        static::assertStringNotContainsString("\x1B[1A", substr($this->terminal->raw($output), $finished));
    }

    public function test_an_entry_that_ends_again_with_no_open_beat_between_shows_its_new_end(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);

        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 7.0, stages: [StageProgressMother::done(
            'sort',
            10,
            0.0,
            6.0,
        )]));
        $heartbeat->beat(PulseMother::pulse(beat: 2, elapsedSeconds: 13.0, stages: [StageProgressMother::done(
            'sort',
            30,
            0.0,
            12.0,
        )]));

        $lines = [];
        preg_match_all('/^  \S sort      (\d+) rows .*$/mu', $this->terminal->text($output), $lines);
        static::assertSame(['10', '30'], $lines[1]);
        static::assertStringEndsWith('12.0s DONE', $lines[0][1]);
    }

    public function test_an_entry_that_opens_again_in_a_failed_run_leaves_no_done_line(): void
    {
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);

        $heartbeat->beat(PulseMother::pulse(elapsedSeconds: 7.0, stages: [StageProgressMother::done(
            'sort',
            10,
            0.0,
            6.0,
        )]));
        $heartbeat->beat(PulseMother::pulse(beat: 2, elapsedSeconds: 8.0, stages: [StageProgressMother::open(
            'sort',
            20,
        )]));
        $heartbeat->finish();

        static::assertStringEndsWith("\x1B[1A\x1B[0J", $this->terminal->raw($output));
    }

    public function test_an_open_line_at_the_top_keeps_the_whole_block_redrawn(): void
    {
        $this->terminal->lines(4);
        $output = $this->terminal->stream();
        $heartbeat = new TaskListHeartbeat($output);
        $stages = [
            StageProgressMother::open('group_by', 1, id: 1),
            StageProgressMother::done('read', 1, id: 2),
            StageProgressMother::done('sort', 1, id: 3),
            StageProgressMother::done('join', 1, id: 4),
            StageProgressMother::done('window', 1, id: 5),
        ];

        $heartbeat->beat(PulseMother::pulse(stages: $stages));
        $heartbeat->beat(PulseMother::pulse(beat: 2, stages: $stages));

        $ups = [];
        preg_match_all('/\x1B\[(\d+)A/', $this->terminal->raw($output), $ups);
        static::assertSame(['5'], $ups[1]);
    }

    public function test_a_line_is_at_most_100_columns_on_a_wider_terminal(): void
    {
        $this->terminal->columns(150);
        $output = $this->terminal->stream();

        (new TaskListHeartbeat($output))->beat(PulseMother::pulse(elapsedSeconds: 2.0, stages: [
            StageProgressMother::open('sort', 15_301_437, 30_000_000, location: 'memory://in/orders-80000000.floe'),
        ]));

        static::assertSame(99, Helper::width(rtrim($this->terminal->text($output), "\n")));
    }

    public function test_memory_drops_before_the_rate(): void
    {
        $this->terminal->columns(56);
        $output = $this->terminal->stream(OutputInterface::VERBOSITY_VERBOSE);

        (new TaskListHeartbeat(
            $output,
        ))->beat(PulseMother::pulse(elapsedSeconds: 2.0, stages: [StageProgressMother::open('sort', 15_301_437)]));

        static::assertSame("  ⠙ sort      15,301,437 rows ........... 7.7M/s · 2.0s\n", $this->terminal->text($output));
    }

    public function test_a_terminal_too_narrow_for_any_dots_gets_the_label_the_count_and_the_time(): void
    {
        $this->terminal->columns(30);
        $output = $this->terminal->stream();

        (new TaskListHeartbeat($output))->beat(PulseMother::pulse(elapsedSeconds: 2.0, stages: [
            StageProgressMother::open('sort', 15_301_437, 30_000_000, location: 'memory://in/orders-80000000.floe'),
        ]));

        static::assertSame("  ⠙ sort      15.3M/30.0M  51% 2.0s\n", $this->terminal->text($output));
    }

    public function test_built_directly_it_draws_on_the_output_it_was_given(): void
    {
        $console = $this->terminal->console();

        (new TaskListHeartbeat(
            $console,
        ))->beat(PulseMother::pulse(elapsedSeconds: 2.0, stages: [StageProgressMother::open()]));

        static::assertSame([true, ''], [
            str_contains($this->terminal->text($console), ' sort '),
            $this->terminal->raw(type_instance_of(StreamOutput::class)->assert($console->getErrorOutput())),
        ]);
    }
}
