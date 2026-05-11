<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Console;

use Flow\Telemetry\Provider\Console\ConsoleOutput;
use PHPUnit\Framework\TestCase;

final class ConsoleOutputTest extends TestCase
{
    public function test_border_generates_correct_width(): void
    {
        $output = new ConsoleOutput(colors: false);
        $border = $output->border(20);

        static::assertSame('+------------------+', $border);
    }

    public function test_color_disabled_returns_plain_text(): void
    {
        $output = new ConsoleOutput(colors: false);

        static::assertSame('test', $output->red('test'));
        static::assertSame('test', $output->green('test'));
        static::assertSame('test', $output->blue('test'));
    }

    public function test_color_enabled_wraps_text_with_ansi(): void
    {
        $output = new ConsoleOutput(colors: true);

        static::assertStringContainsString("\033[", $output->red('test'));
        static::assertStringContainsString('test', $output->red('test'));
    }

    public function test_format_duration_microseconds(): void
    {
        $output = new ConsoleOutput();

        static::assertStringContainsString('us', $output->formatDuration(0.5));
    }

    public function test_format_duration_milliseconds(): void
    {
        $output = new ConsoleOutput();

        static::assertStringContainsString('ms', $output->formatDuration(50.5));
    }

    public function test_format_duration_null_returns_dash(): void
    {
        $output = new ConsoleOutput();

        static::assertSame('-', $output->formatDuration(null));
    }

    public function test_format_duration_seconds(): void
    {
        $output = new ConsoleOutput();

        static::assertStringContainsString('s', $output->formatDuration(5000.0));
    }

    public function test_format_timestamp(): void
    {
        $output = new ConsoleOutput();
        $dt = new \DateTimeImmutable('2024-01-15 10:30:45.123456');

        $result = $output->formatTimestamp($dt);

        static::assertSame('2024-01-15 10:30:45.123456', $result);
    }

    public function test_format_value_array(): void
    {
        $output = new ConsoleOutput();

        static::assertSame('["a","b"]', $output->formatValue(['a', 'b']));
    }

    public function test_format_value_bool(): void
    {
        $output = new ConsoleOutput();

        static::assertSame('true', $output->formatValue(true));
        static::assertSame('false', $output->formatValue(false));
    }

    public function test_format_value_null(): void
    {
        $output = new ConsoleOutput();

        static::assertSame('null', $output->formatValue(null));
    }

    public function test_format_value_number(): void
    {
        $output = new ConsoleOutput();

        static::assertSame('42', $output->formatValue(42));
        static::assertSame('3.14', $output->formatValue(3.14));
    }

    public function test_format_value_string(): void
    {
        $output = new ConsoleOutput();

        static::assertSame('hello', $output->formatValue('hello'));
    }

    public function test_pad_left(): void
    {
        $output = new ConsoleOutput();

        static::assertSame('   abc', $output->pad('abc', 6, ' ', STR_PAD_LEFT));
    }

    public function test_pad_right(): void
    {
        $output = new ConsoleOutput();

        static::assertSame('abc   ', $output->pad('abc', 6, ' ', STR_PAD_RIGHT));
    }

    public function test_row_generates_bordered_content(): void
    {
        $output = new ConsoleOutput(colors: false);
        $row = $output->row('test', 20);

        static::assertSame('| test             |', $row);
    }

    public function test_strip_colors_removes_ansi(): void
    {
        $output = new ConsoleOutput();
        $colored = $output->red('test');

        static::assertSame('test', $output->stripColors($colored));
    }

    public function test_truncate_long_text(): void
    {
        $output = new ConsoleOutput();

        static::assertSame('hello...', $output->truncate('hello world this is a long text', 8));
    }

    public function test_truncate_short_text_unchanged(): void
    {
        $output = new ConsoleOutput();

        static::assertSame('hello', $output->truncate('hello', 10));
    }

    public function test_write_to_stream(): void
    {
        $stream = \fopen('php://memory', 'rwb');
        static::assertIsResource($stream);
        $output = new ConsoleOutput(colors: false, stream: $stream);

        $output->write('test');

        \rewind($stream);
        static::assertSame("test\n", \stream_get_contents($stream));
        \fclose($stream);
    }
}
