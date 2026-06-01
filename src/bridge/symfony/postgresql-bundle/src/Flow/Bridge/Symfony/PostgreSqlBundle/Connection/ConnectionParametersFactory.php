<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Connection;

use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\DsnParser;

final class ConnectionParametersFactory
{
    /**
     * @param array{dbname?: ?string, host?: ?string, port?: ?int, user?: ?string, password?: ?string, dbname_suffix?: string} $overrides
     */
    public static function create(DsnParser $parser, string $dsn, array $overrides = []): ConnectionParameters
    {
        $params = $parser->parse($dsn);

        if (($overrides['dbname'] ?? null) !== null) {
            $params = $params->withDatabase($overrides['dbname']);
        }

        if (($overrides['host'] ?? null) !== null) {
            $params = $params->withHost($overrides['host']);
        }

        if (($overrides['port'] ?? null) !== null) {
            $params = $params->withPort($overrides['port']);
        }

        if (($overrides['user'] ?? null) !== null) {
            $params = $params->withUser($overrides['user']);
        }

        if (($overrides['password'] ?? null) !== null) {
            $params = $params->withPassword($overrides['password']);
        }

        return $params->withDatabaseSuffix($overrides['dbname_suffix'] ?? '');
    }
}
