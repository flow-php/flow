<?php

declare(strict_types=1);

namespace Flow\ETL\Tests;

trait CommandOutputNormalizer
{
    /**
     * Assert that command output contains expected string, normalizing line endings.
     *
     * @param string $expected The expected string (with Unix line endings)
     * @param string $actual The actual command output
     * @param string $message Optional failure message
     */
    protected static function assertCommandOutputContains(string $expected, string $actual, string $message = '') : void
    {
        self::assertStringContainsString($expected, self::normalizeCommandOutput($actual), $message);
    }

    /**
     * Assert that command output equals expected string, normalizing line endings.
     *
     * @param string $expected The expected string (with Unix line endings)
     * @param string $actual The actual command output
     * @param string $message Optional failure message
     */
    protected static function assertCommandOutputEquals(string $expected, string $actual, string $message = '') : void
    {
        self::assertEquals($expected, self::normalizeCommandOutput($actual), $message);
    }

    /**
     * Assert that command output is identical to expected string, normalizing line endings.
     *
     * @param string $expected The expected string (with Unix line endings)
     * @param string $actual The actual command output
     * @param string $message Optional failure message
     */
    protected static function assertCommandOutputIdentical(string $expected, string $actual, string $message = '') : void
    {
        self::assertSame($expected, self::normalizeCommandOutput($actual), $message);
    }

    /**
     * Normalize command output to use Unix line endings and remove ANSI color codes for consistent testing across platforms.
     *
     * @param string $output The command output to normalize
     *
     * @return string The normalized output with Unix line endings and no ANSI codes
     */
    protected static function normalizeCommandOutput(string $output) : string
    {
        $output = \str_replace("\r\n", "\n", $output);

        $output = \preg_replace('/\x1b\[[0-9;]*m/', '', $output);

        $lines = \explode("\n", (string) $output);
        $lines = \array_map('rtrim', $lines);

        return \implode("\n", $lines);
    }
}
