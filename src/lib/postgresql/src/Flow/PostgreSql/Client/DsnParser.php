<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

/**
 * Parses PostgreSQL DSN (Data Source Name) strings into ConnectionParameters.
 *
 * Supports standard PostgreSQL URL formats commonly used in environment variables:
 * - postgres://user:password@host:port/database?options
 * - postgresql://user:password@host:port/database?options
 */
final readonly class DsnParser
{
    /**
     * Parse a DSN string into ConnectionParameters.
     *
     * @throws DsnParserException If the DSN cannot be parsed
     */
    public function parse(#[\SensitiveParameter] string $dsn) : ConnectionParameters
    {
        $params = $this->parseDsn($dsn);

        return ConnectionParameters::fromParams(
            database: $params['dbname'],
            host: $params['host'],
            port: $params['port'],
            user: $params['user'],
            password: $params['password'],
            options: $params['options'],
        );
    }

    /**
     * @param array<string, mixed> $parts
     */
    private function parseDatabase(array $parts) : string
    {
        $path = isset($parts['path']) && \is_string($parts['path']) ? $parts['path'] : '';
        $database = \ltrim($path, '/');

        if ($database === '') {
            throw DsnParserException::missingDatabase();
        }

        return $database;
    }

    /**
     * @throws DsnParserException
     *
     * @return array{
     *     dbname: string,
     *     host: string,
     *     port: int,
     *     user: ?string,
     *     password: ?string,
     *     options: array<string, string>
     * }
     */
    private function parseDsn(string $dsn) : array
    {
        $this->validateScheme($dsn);

        $parts = \parse_url($dsn);

        if ($parts === false) {
            throw DsnParserException::invalidDsn($dsn);
        }

        return [
            'dbname' => $this->parseDatabase($parts),
            'host' => $parts['host'] ?? 'localhost',
            'port' => $parts['port'] ?? 5432,
            'user' => isset($parts['user']) ? \urldecode($parts['user']) : null,
            'password' => isset($parts['pass']) ? \urldecode($parts['pass']) : null,
            'options' => $this->parseOptions($parts),
        ];
    }

    /**
     * @param array<string, mixed> $parts
     *
     * @return array<string, string>
     */
    private function parseOptions(array $parts) : array
    {
        if (!isset($parts['query']) || !\is_string($parts['query'])) {
            return [];
        }

        $parsed = [];
        \parse_str($parts['query'], $parsed);

        $validOptions = [
            'application_name', 'channel_binding', 'client_encoding', 'connect_timeout',
            'fallback_application_name', 'gssencmode', 'gsslib', 'hostaddr',
            'keepalives', 'keepalives_count', 'keepalives_idle', 'keepalives_interval',
            'krbsrvname', 'options', 'passfile', 'replication', 'requirepeer', 'requiressl',
            'service', 'ssl_max_protocol_version', 'ssl_min_protocol_version',
            'sslcert', 'sslcompression', 'sslcrl', 'sslcrldir', 'sslkey', 'sslmode',
            'sslrootcert', 'sslsni', 'target_session_attrs', 'tcp_user_timeout',
        ];

        $options = [];

        foreach ($parsed as $key => $value) {
            if (\in_array($key, $validOptions, true) && \is_string($value)) {
                $options[$key] = $value;
            }
        }

        return $options;
    }

    private function validateScheme(string $dsn) : void
    {
        $validSchemes = ['postgres://', 'postgresql://', 'pgsql://'];

        foreach ($validSchemes as $scheme) {
            if (\str_starts_with($dsn, $scheme)) {
                return;
            }
        }

        throw DsnParserException::unsupportedScheme($dsn);
    }
}
