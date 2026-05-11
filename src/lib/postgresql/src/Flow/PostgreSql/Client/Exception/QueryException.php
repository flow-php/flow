<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

final class QueryException extends ClientException
{
    private const int SQL_PREVIEW_LENGTH = 100;

    private function __construct(
        string $message,
        private readonly string $sql,
        private readonly PostgreSqlError $error,
    ) {
        parent::__construct($message);
    }

    public static function executionFailed(string $sql, PostgreSqlError $error): self
    {
        $sqlPreview = \strlen($sql) > self::SQL_PREVIEW_LENGTH
            ? \substr($sql, 0, self::SQL_PREVIEW_LENGTH) . '...'
            : $sql;

        return new self(
            \sprintf('Query execution failed [%s]: %s. SQL: %s', $error->sqlState, $error->safeMessage(), $sqlPreview),
            $sql,
            $error,
        );
    }

    public function error(): PostgreSqlError
    {
        return $this->error;
    }

    public function sql(): string
    {
        return $this->sql;
    }
}
