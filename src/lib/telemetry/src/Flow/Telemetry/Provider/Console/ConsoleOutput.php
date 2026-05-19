<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Console;

use DateTimeImmutable;
use JsonException;

use function floor;
use function fwrite;
use function is_array;
use function is_bool;
use function json_encode;
use function mb_strlen;
use function mb_substr;
use function preg_replace;
use function sprintf;
use function str_repeat;

use const STDOUT;

final class ConsoleOutput
{
    private const string BLUE = "\033[34m";

    private const string BOLD = "\033[1m";

    private const string CYAN = "\033[36m";

    private const string DIM = "\033[2m";

    private const string GRAY = "\033[90m";

    private const string GREEN = "\033[32m";

    private const string RED = "\033[31m";

    private const string RESET = "\033[0m";

    private const string YELLOW = "\033[33m";

    /** @var resource */
    private $stream;

    /**
     * @param bool $colors Whether to use ANSI colors
     * @param null|resource $stream Output stream (default: STDOUT)
     */
    public function __construct(
        private readonly bool $colors = true,
        $stream = null,
    ) {
        $this->stream = $stream ?? STDOUT;
    }

    public function blue(string $text): string
    {
        return $this->color($text, self::BLUE);
    }

    public function bold(string $text): string
    {
        return $this->color($text, self::BOLD);
    }

    public function border(int $width): string
    {
        return '+' . str_repeat('-', $width - 2) . '+';
    }

    public function cyan(string $text): string
    {
        return $this->color($text, self::CYAN);
    }

    public function dim(string $text): string
    {
        return $this->color($text, self::DIM);
    }

    public function formatDuration(?float $ms): string
    {
        if ($ms === null) {
            return '-';
        }

        if ($ms < 1) {
            return sprintf('%.2fus', $ms * 1000);
        }

        if ($ms < 1000) {
            return sprintf('%.2fms', $ms);
        }

        return sprintf('%.2fs', $ms / 1000);
    }

    public function formatTimestamp(DateTimeImmutable $dt): string
    {
        return $dt->format('Y-m-d H:i:s.u');
    }

    /**
     * @param null|array<mixed>|bool|float|int|string $value
     */
    public function formatValue(array|bool|float|int|string|null $value): string
    {
        if ($value === null) {
            return 'null';
        }

        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        if (is_array($value)) {
            try {
                return json_encode($value, JSON_THROW_ON_ERROR);
            } catch (JsonException) {
                return '{...}';
            }
        }

        return (string) $value;
    }

    public function gray(string $text): string
    {
        return $this->color($text, self::GRAY);
    }

    public function green(string $text): string
    {
        return $this->color($text, self::GREEN);
    }

    /**
     * Multi-byte safe string padding that accounts for ANSI color codes.
     *
     * @see Flow\ETL\Formatter\ASCII\ASCIIValue::mb_str_pad
     */
    public function pad(string $input, int $length, string $padding = ' ', int $padType = STR_PAD_RIGHT): string
    {
        $visibleLength = mb_strlen($this->stripColors($input));
        $paddingRequired = $length - $visibleLength;

        if ($paddingRequired <= 0) {
            return $input;
        }

        return match ($padType) {
            STR_PAD_LEFT => mb_substr(str_repeat($padding, $paddingRequired), 0, $paddingRequired) . $input,
            STR_PAD_BOTH => mb_substr(
                str_repeat($padding, (int) floor($paddingRequired / 2)),
                0,
                (int) floor($paddingRequired / 2),
            )
                . $input
                . mb_substr(
                    str_repeat($padding, $paddingRequired - (int) floor($paddingRequired / 2)),
                    0,
                    $paddingRequired - (int) floor($paddingRequired / 2),
                ),
            default => $input . mb_substr(str_repeat($padding, $paddingRequired), 0, $paddingRequired),
        };
    }

    public function red(string $text): string
    {
        return $this->color($text, self::RED);
    }

    public function row(string $content, int $width): string
    {
        $contentLength = mb_strlen($this->stripColors($content));
        $padding = $width - 4 - $contentLength;

        if ($padding < 0) {
            $padding = 0;
        }

        return '| ' . $content . str_repeat(' ', $padding) . ' |';
    }

    public function stripColors(string $text): string
    {
        return (string) preg_replace('/\033\[[0-9;]*m/', '', $text);
    }

    /**
     * Truncate text to a maximum visible length, accounting for ANSI codes.
     */
    public function truncate(string $text, int $max): string
    {
        $stripped = $this->stripColors($text);

        if (mb_strlen($stripped) <= $max) {
            return $text;
        }

        return mb_substr($stripped, 0, $max - 3) . '...';
    }

    public function write(string $text): void
    {
        fwrite($this->stream, $text . PHP_EOL);
    }

    public function yellow(string $text): string
    {
        return $this->color($text, self::YELLOW);
    }

    private function color(string $text, string $code): string
    {
        if (!$this->colors) {
            return $text;
        }

        return $code . $text . self::RESET;
    }
}
