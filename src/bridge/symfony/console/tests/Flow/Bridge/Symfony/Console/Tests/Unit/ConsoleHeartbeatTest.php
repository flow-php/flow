<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\Tests\Unit;

use Flow\Bridge\Symfony\Console\Tests\Mother\PulseMother;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;
use Symfony\Component\Console\Formatter\OutputFormatter;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Output\ConsoleOutput;
use Symfony\Component\Console\Output\ConsoleSectionOutput;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Output\StreamOutput;

use function Flow\Bridge\Symfony\Console\DSL\console_heartbeat;
use function fopen;
use function getenv;
use function putenv;
use function rewind;
use function stream_get_contents;
use function substr_count;

final class ConsoleHeartbeatTest extends FlowTestCase
{
    public false|string $columns = false;

    protected function setUp(): void
    {
        parent::setUp();
        $this->columns = getenv('COLUMNS');
        putenv('COLUMNS=120');
    }

    protected function tearDown(): void
    {
        putenv($this->columns === false ? 'COLUMNS' : 'COLUMNS=' . $this->columns);
        parent::tearDown();
    }

    public function test_a_beat_after_finish_starts_a_new_indicator(): void
    {
        $output = new BufferedOutput();
        $heartbeat = console_heartbeat($output);
        $heartbeat->beat(PulseMother::pulse(beat: 1));
        $heartbeat->finish();
        $output->fetch();

        $heartbeat->beat(PulseMother::pulse(beat: 2, stage: 'join'));

        static::assertStringStartsWith(' join: 1,250,000 rows · ', $output->fetch());
    }

    public function test_a_console_output_renders_on_stderr(): void
    {
        $errorOutput = new BufferedOutput();
        $output = new ConsoleOutput();
        $output->setErrorOutput($errorOutput);

        console_heartbeat($output)->beat(PulseMother::pulse());

        static::assertSame(
            " sort: 1,250,000 rows · 2,000,000 read · 142.0 MiB (peak 310.0 MiB) · 1 min, 23 s\n",
            $errorOutput->fetch(),
        );
    }

    public function test_a_plain_output_gets_one_line_per_beat(): void
    {
        $output = new BufferedOutput();
        $heartbeat = console_heartbeat($output);

        $heartbeat->beat(PulseMother::pulse(beat: 1));
        $heartbeat->beat(PulseMother::pulse(
            beat: 2,
            stage: 'join',
            stageRows: 1_500_000,
            rowsRead: 3_000_000,
            memoryBytes: 1_024,
            peakMemoryBytes: 2_048,
            elapsedSeconds: 3_725,
        ));

        static::assertSame(
            " sort: 1,250,000 rows · 2,000,000 read · 142.0 MiB (peak 310.0 MiB) · 1 min, 23 s\n"
            . " join: 1,500,000 rows · 3,000,000 read · 1 KiB (peak 2 KiB) · 1 h, 2 min\n",
            $output->fetch(),
        );
    }

    public function test_a_quiet_output_prints_nothing(): void
    {
        $output = new BufferedOutput(OutputInterface::VERBOSITY_QUIET);
        $heartbeat = console_heartbeat($output);

        $heartbeat->beat(PulseMother::pulse());
        $heartbeat->finish();

        static::assertSame('', $output->fetch());
    }

    public function test_a_second_finish_does_nothing(): void
    {
        $stream = fopen('php://memory', 'w+b');
        $output = new StreamOutput($stream, OutputInterface::VERBOSITY_NORMAL, true);
        $heartbeat = console_heartbeat($output);
        $heartbeat->beat(PulseMother::pulse());

        $heartbeat->finish();
        $output->write('command line');

        $heartbeat->finish();

        rewind($stream);
        static::assertSame(
            "\x0D\x1B[2K - sort: 1,250,000 rows · 2,000,000 read · 142.0 MiB (peak 310.0 MiB) · 1 min, 23 s\x1B[1G\x1B[2K"
            . 'command line',
            stream_get_contents($stream),
        );
    }

    public function test_a_section_is_overwritten_in_place(): void
    {
        $sections = [];
        $output = new ConsoleSectionOutput(
            fopen('php://memory', 'w+b'),
            $sections,
            OutputInterface::VERBOSITY_NORMAL,
            true,
            new OutputFormatter(),
        );
        $heartbeat = console_heartbeat($output);

        $heartbeat->beat(PulseMother::pulse(beat: 1));
        $heartbeat->beat(PulseMother::pulse(beat: 2, stageRows: 1_500_000));

        static::assertStringEndsWith(
            " sort: 1,500,000 rows · 2,000,000 read · 142.0 MiB (peak 310.0 MiB) · 1 min, 23 s\n",
            $output->getContent(),
        );
        static::assertSame(1, substr_count($output->getContent(), "\n"));
    }

    public function test_a_single_row_is_singular(): void
    {
        $output = new BufferedOutput();

        console_heartbeat($output)->beat(PulseMother::pulse(stageRows: 1));

        static::assertStringStartsWith(' sort: 1 row · 2,000,000 read · ', $output->fetch());
    }

    public function test_a_single_file_operation_is_singular(): void
    {
        $output = new BufferedOutput();

        console_heartbeat($output)->beat(PulseMother::pulse(stage: 'commit', stageRows: 1));

        static::assertStringStartsWith(' commit: 1 file operation · ', $output->fetch());
    }

    public function test_a_stage_with_console_tags_prints_them_literally(): void
    {
        $output = new BufferedOutput();

        console_heartbeat($output)->beat(PulseMother::pulse(stage: '<error>x</error>'));

        static::assertStringStartsWith(' <error>x</error>: 1,250,000 rows · ', $output->fetch());
    }

    public function test_a_terminal_redraws_the_line(): void
    {
        $stream = fopen('php://memory', 'w+b');
        $heartbeat = console_heartbeat(new StreamOutput($stream, OutputInterface::VERBOSITY_NORMAL, true));
        $heartbeat->beat(PulseMother::pulse(beat: 1));
        $heartbeat->beat(PulseMother::pulse(beat: 2, stageRows: 1_500_000));

        rewind($stream);
        static::assertStringStartsWith(
            "\x0D\x1B[2K - sort: 1,250,000 rows · 2,000,000 read · 142.0 MiB (peak 310.0 MiB) · 1 min, 23 s"
            . "\x0D\x1B[2K - sort: 1,500,000 rows · 2,000,000 read · 142.0 MiB (peak 310.0 MiB) · 1 min, 23 s",
            (string) stream_get_contents($stream),
        );
    }

    public function test_a_terminal_line_is_cut_to_the_terminal_width(): void
    {
        $stream = fopen('php://memory', 'w+b');
        putenv('COLUMNS=60');

        console_heartbeat(new StreamOutput($stream, OutputInterface::VERBOSITY_NORMAL, true))
            ->beat(PulseMother::pulse());

        rewind($stream);
        static::assertSame(
            "\x0D\x1B[2K - sort: 1,250,000 rows · 2,000,000 read · 142.0 MiB (peak ",
            stream_get_contents($stream),
        );
    }

    public function test_a_terminal_narrower_than_the_indicator_keeps_one_column(): void
    {
        $stream = fopen('php://memory', 'w+b');
        putenv('COLUMNS=2');

        console_heartbeat(new StreamOutput($stream, OutputInterface::VERBOSITY_NORMAL, true))
            ->beat(PulseMother::pulse());

        rewind($stream);
        static::assertSame("\x0D\x1B[2K - s", stream_get_contents($stream));
    }

    public function test_a_section_line_is_not_cut(): void
    {
        $sections = [];
        $output = new ConsoleSectionOutput(
            fopen('php://memory', 'w+b'),
            $sections,
            OutputInterface::VERBOSITY_NORMAL,
            true,
            new OutputFormatter(),
        );
        putenv('COLUMNS=60');

        console_heartbeat($output)->beat(PulseMother::pulse());

        static::assertSame(
            " - sort: 1,250,000 rows · 2,000,000 read · 142.0 MiB (peak 310.0 MiB) · 1 min, 23 s\n",
            $output->getContent(),
        );
    }

    public function test_an_empty_stage_reads_starting(): void
    {
        $output = new BufferedOutput();

        console_heartbeat($output)->beat(PulseMother::pulse(stage: ''));

        static::assertStringStartsWith(' starting: 1,250,000 rows · ', $output->fetch());
    }

    public function test_the_commit_stage_counts_file_operations(): void
    {
        $output = new BufferedOutput();

        console_heartbeat($output)->beat(PulseMother::pulse(stage: 'commit', stageRows: 12));

        static::assertStringStartsWith(' commit: 12 file operations · ', $output->fetch());
    }

    #[TestWith([0, '0 s'])]
    #[TestWith([1, '1 s'])]
    #[TestWith([59, '59 s'])]
    #[TestWith([60, '1 min'])]
    #[TestWith([83, '1 min, 23 s'])]
    #[TestWith([3_601, '1 h'])]
    #[TestWith([3_660, '1 h, 1 min'])]
    #[TestWith([90_061, '1 d, 1 h'])]
    #[TestWith([172_805, '2 d'])]
    public function test_elapsed_time_shows_the_two_largest_units(int $seconds, string $elapsed): void
    {
        $output = new BufferedOutput();

        console_heartbeat($output)->beat(PulseMother::pulse(elapsedSeconds: $seconds));

        static::assertStringEndsWith(" (peak 310.0 MiB) · {$elapsed}\n", $output->fetch());
    }

    public function test_finish_clears_a_section(): void
    {
        $sections = [];
        $output = new ConsoleSectionOutput(
            fopen('php://memory', 'w+b'),
            $sections,
            OutputInterface::VERBOSITY_NORMAL,
            true,
            new OutputFormatter(),
        );
        $heartbeat = console_heartbeat($output);
        $heartbeat->beat(PulseMother::pulse());

        $heartbeat->finish();

        static::assertSame('', $output->getContent());
    }

    public function test_finish_on_a_plain_output_adds_nothing(): void
    {
        $output = new BufferedOutput();
        $heartbeat = console_heartbeat($output);
        $heartbeat->beat(PulseMother::pulse());

        $heartbeat->finish();

        static::assertSame(
            " sort: 1,250,000 rows · 2,000,000 read · 142.0 MiB (peak 310.0 MiB) · 1 min, 23 s\n",
            $output->fetch(),
        );
    }

    public function test_finish_on_a_terminal_clears_the_status_line(): void
    {
        $stream = fopen('php://memory', 'w+b');
        $heartbeat = console_heartbeat(new StreamOutput($stream, OutputInterface::VERBOSITY_NORMAL, true));
        $heartbeat->beat(PulseMother::pulse());

        $heartbeat->finish();

        rewind($stream);
        static::assertSame(
            "\x0D\x1B[2K - sort: 1,250,000 rows · 2,000,000 read · 142.0 MiB (peak 310.0 MiB) · 1 min, 23 s\x1B[1G\x1B[2K",
            stream_get_contents($stream),
        );
    }

    public function test_finish_without_a_beat_does_nothing(): void
    {
        $stream = fopen('php://memory', 'w+b');
        $output = new StreamOutput($stream, OutputInterface::VERBOSITY_NORMAL, true);
        $output->write('command line');

        console_heartbeat($output)->finish();

        rewind($stream);
        static::assertSame('command line', stream_get_contents($stream));
    }
}
