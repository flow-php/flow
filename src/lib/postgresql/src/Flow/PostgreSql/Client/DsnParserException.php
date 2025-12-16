<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

final class DsnParserException extends \InvalidArgumentException
{
    public static function invalidDsn(string $dsn) : self
    {
        return new self(\sprintf('Invalid DSN: "%s"', self::maskPassword($dsn)));
    }

    public static function missingDatabase() : self
    {
        return new self('DSN must specify a database name');
    }

    public static function unsupportedScheme(string $dsn) : self
    {
        return new self(\sprintf(
            'Unsupported DSN scheme. Expected "postgres://", "postgresql://", or "pgsql://", got: "%s"',
            self::maskPassword($dsn)
        ));
    }

    /**
     * Mask password in DSN for safe display in error messages.
     *
     * Handles standard URL format: scheme://user:password@host:port/database
     * Supports URL-encoded passwords (e.g., p%40ssword for p@ssword).
     */
    private static function maskPassword(string $dsn) : string
    {
        return \preg_replace(
            '/(:\/\/[^:]*):([^@]*)@/',
            '$1:***@',
            $dsn
        ) ?? $dsn;
    }
}
