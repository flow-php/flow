<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

final class QueryException extends ClientException
{
    public static function columnNotFound(string $column) : self
    {
        return new self(\sprintf('Column "%s" not found in result set', $column));
    }

    public static function executionFailed(string $sql, string $error) : self
    {
        return new self(\sprintf('Query execution failed: %s. SQL: %s', $error, $sql));
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
}
