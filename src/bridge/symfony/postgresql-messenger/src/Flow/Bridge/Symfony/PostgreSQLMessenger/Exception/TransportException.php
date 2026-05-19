<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Exception;

use Symfony\Component\Messenger\Exception\TransportException as SymfonyTransportException;

use function sprintf;

final class TransportException extends SymfonyTransportException
{
    public static function unexpectedRowShape(string $field, string $actualType): self
    {
        return new self(sprintf(
            'Unexpected messenger row shape: field "%s" must be int or string, got %s',
            $field,
            $actualType,
        ));
    }
}
