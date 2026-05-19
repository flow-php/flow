<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession\Exception;

use RuntimeException;

use function sprintf;

final class SessionException extends RuntimeException
{
    public static function unexpectedRowShape(string $field, string $actualType): self
    {
        return new self(sprintf(
            'Unexpected session row shape: field "%s" must be string, got %s',
            $field,
            $actualType,
        ));
    }
}
