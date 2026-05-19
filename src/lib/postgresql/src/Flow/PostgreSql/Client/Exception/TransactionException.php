<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

use function sprintf;

final class TransactionException extends ClientException
{
    public static function beginFailed(string $error): self
    {
        return new self(sprintf('Failed to begin transaction: %s', $error));
    }

    public static function commitFailed(string $error): self
    {
        return new self(sprintf('Failed to commit transaction: %s', $error));
    }

    public static function noActiveTransaction(): self
    {
        return new self('There is no active transaction');
    }

    public static function releaseSavepointFailed(string $name, string $error): self
    {
        return new self(sprintf('Failed to release savepoint "%s": %s', $name, $error));
    }

    public static function rollbackFailed(string $error): self
    {
        return new self(sprintf('Failed to rollback transaction: %s', $error));
    }

    public static function rollbackToSavepointFailed(string $name, string $error): self
    {
        return new self(sprintf('Failed to rollback to savepoint "%s": %s', $name, $error));
    }

    public static function savepointFailed(string $name, string $error): self
    {
        return new self(sprintf('Failed to create savepoint "%s": %s', $name, $error));
    }
}
