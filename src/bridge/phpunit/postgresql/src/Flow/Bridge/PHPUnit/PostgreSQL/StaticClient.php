<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL;

use Flow\PostgreSql\Client\{Client, ConnectionParameters};
use Flow\PostgreSql\Client\Infrastructure\PgSql\PgSqlClient;
use Flow\PostgreSql\Client\Types\ValueConverters;

final class StaticClient
{
    /** @var array<string, Client> */
    private static array $clients = [];

    private static bool $enabled = false;

    private static bool $transactionActive = false;

    private function __construct()
    {
    }

    public static function beginTransaction() : void
    {
        self::$transactionActive = true;

        foreach (self::$clients as $client) {
            if ($client->getTransactionNestingLevel() === 0) {
                $client->beginTransaction();
            }
        }
    }

    public static function closeAll() : void
    {
        foreach (self::$clients as $client) {
            if ($client->isConnected()) {
                while ($client->getTransactionNestingLevel() > 0) {
                    $client->rollBack();
                }

                $client->close();
            }
        }

        self::$clients = [];
    }

    public static function connect(ConnectionParameters $params, ?ValueConverters $converters = null) : Client
    {
        if (!self::$enabled) {
            return PgSqlClient::connect($params, $converters);
        }

        $key = \sha1(\sprintf('%s:%d:%s:%s', $params->host(), $params->port(), $params->database(), $params->user() ?? ''));

        if (!\array_key_exists($key, self::$clients)) {
            self::$clients[$key] = PgSqlClient::connect($params, $converters);

            if (self::$transactionActive) {
                self::$clients[$key]->beginTransaction();
            }
        }

        return self::$clients[$key];
    }

    public static function disable() : void
    {
        self::$enabled = false;
    }

    public static function enable() : void
    {
        self::$enabled = true;
    }

    public static function isEnabled() : bool
    {
        return self::$enabled;
    }

    public static function reset() : void
    {
        self::$clients = [];
        self::$enabled = false;
        self::$transactionActive = false;
    }

    public static function rollBack() : void
    {
        self::$transactionActive = false;

        foreach (self::$clients as $client) {
            while ($client->getTransactionNestingLevel() > 0) {
                $client->rollBack();
            }
        }
    }
}
