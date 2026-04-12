<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

final class ConnectionException extends ClientException
{
    public static function connectionFailed(string $error) : self
    {
        return new self(\sprintf('Failed to connect to PostgreSQL: %s', $error));
    }

    public static function extensionNotLoaded(string $extension) : self
    {
        return new self(\sprintf('PHP extension "%s" is not loaded', $extension));
    }

    public static function notConnected() : self
    {
        return new self('Not connected to PostgreSQL server');
    }

    public static function notificationWaitFailed(string $error) : self
    {
        return new self(\sprintf('Failed to wait for PostgreSQL notification: %s', $error));
    }
}
