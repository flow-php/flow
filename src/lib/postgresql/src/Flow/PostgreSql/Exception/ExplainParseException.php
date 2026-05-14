<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Exception;

final class ExplainParseException extends \RuntimeException
{
    public static function invalidJson(string $message): self
    {
        return new self("Invalid EXPLAIN JSON output: {$message}");
    }

    public static function missingField(string $field): self
    {
        return new self("Missing required field in EXPLAIN output: {$field}");
    }

    public static function unexpectedFormat(string $expected, string $actual, ?\Throwable $previous = null): self
    {
        return new self("Unexpected EXPLAIN format: expected {$expected}, got {$actual}", 0, $previous);
    }
}
