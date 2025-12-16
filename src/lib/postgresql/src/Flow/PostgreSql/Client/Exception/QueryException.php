<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

final class QueryException extends ClientException
{
    private const int SQL_PREVIEW_LENGTH = 100;

    private ?string $sql = null;

    public static function columnNotFound(string $column) : self
    {
        return new self(\sprintf('Column "%s" not found in result set', $column));
    }

    public static function executionFailed(string $sql, string $error) : self
    {
        $sqlPreview = \strlen($sql) > self::SQL_PREVIEW_LENGTH
            ? \substr($sql, 0, self::SQL_PREVIEW_LENGTH) . '...'
            : $sql;

        $exception = new self(\sprintf('Query execution failed: %s. SQL: %s', $error, $sqlPreview));
        $exception->sql = $sql;

        return $exception;
    }

    public static function noRowsFound() : self
    {
        return new self('Expected exactly one row, but none were returned');
    }

    public static function sequenceNotUsed(string $sequenceName) : self
    {
        return new self(\sprintf('Sequence "%s" has not been used in this session', $sequenceName));
    }

    public static function tooManyRows(int $count) : self
    {
        return new self(\sprintf('Expected exactly one row, but %d were returned', $count));
    }

    /**
     * Get the full SQL query that caused the exception.
     *
     * Note: This is only available for executionFailed exceptions.
     * Use with caution - do not log or expose to users as it may contain sensitive data.
     */
    public function sql() : ?string
    {
        return $this->sql;
    }
}
