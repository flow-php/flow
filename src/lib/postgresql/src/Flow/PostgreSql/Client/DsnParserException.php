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

    private static function maskPassword(string $dsn) : string
    {
        return \preg_replace('/:([^@\/]+)@/', ':***@', $dsn) ?? $dsn;
    }
}
