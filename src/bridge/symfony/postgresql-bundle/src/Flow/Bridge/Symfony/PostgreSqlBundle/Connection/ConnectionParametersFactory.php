<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Connection;

use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\DsnParser;
use InvalidArgumentException;

use function get_debug_type;
use function is_int;
use function is_string;
use function sprintf;

/**
 * @type ConnectionOverrides = array{dbname?: ?string, host?: ?string, port?: ?int, user?: ?string, password?: ?string, dbname_suffix?: string, options?: array<string, bool|float|int|string|null>, session_settings?: array<string, bool|float|int|string|null>}
 */
final class ConnectionParametersFactory
{
    /**
     * @param ConnectionOverrides $overrides
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

        foreach ($overrides['options'] ?? [] as $key => $value) {
            if (!is_string($value) && !is_int($value)) {
                throw new InvalidArgumentException(sprintf(
                    'Connection option "%s" must be a string or an integer, %s given.',
                    $key,
                    get_debug_type($value),
                ));
            }

            $params = $params->withOption($key, (string) $value);
        }

        $sessionSettings = [];

        foreach ($overrides['session_settings'] ?? [] as $name => $value) {
            if (!is_string($value) && !is_int($value)) {
                throw new InvalidArgumentException(sprintf(
                    'Session setting "%s" must be a string or an integer, %s given.',
                    $name,
                    get_debug_type($value),
                ));
            }

            $sessionSettings[$name] = (string) $value;
        }

        return $params->withSessionSettings($sessionSettings)->withDatabaseSuffix($overrides['dbname_suffix'] ?? '');
    }
}
