<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use InvalidArgumentException;
use SensitiveParameter;

use function array_diff_key;
use function array_flip;
use function array_key_exists;
use function array_merge;
use function ctype_space;
use function implode;
use function preg_match;
use function preg_replace;
use function sprintf;
use function str_replace;
use function strlen;

final readonly class ConnectionParameters
{
    public const array OPTION_KEYS = [
        'application_name',
        'channel_binding',
        'client_encoding',
        'connect_timeout',
        'fallback_application_name',
        'gssdelegation',
        'gssencmode',
        'gsslib',
        'hostaddr',
        'keepalives',
        'keepalives_count',
        'keepalives_idle',
        'keepalives_interval',
        'krbsrvname',
        'load_balance_hosts',
        'max_protocol_version',
        'min_protocol_version',
        'oauth_client_id',
        'oauth_client_secret',
        'oauth_issuer',
        'oauth_scope',
        'options',
        'passfile',
        'replication',
        'require_auth',
        'requirepeer',
        'requiressl',
        'scram_client_key',
        'scram_server_key',
        'service',
        'ssl_max_protocol_version',
        'ssl_min_protocol_version',
        'sslcert',
        'sslcertmode',
        'sslcompression',
        'sslcrl',
        'sslcrldir',
        'sslkey',
        'sslkeylogfile',
        'sslmode',
        'sslnegotiation',
        'sslpassword',
        'sslrootcert',
        'sslsni',
        'target_session_attrs',
        'tcp_user_timeout',
    ];

    /**
     * @param array<string, string> $options
     */
    private function __construct(
        private string $host,
        private int $port,
        private string $database,
        private ?string $user,
        #[SensitiveParameter]
        private ?string $password,
        private array $options,
    ) {}

    /**
     * Create from individual parameters.
     *
     * @param array<string, string> $options Additional connection options (e.g., sslmode, connect_timeout)
     */
    public static function fromParams(
        string $database,
        string $host = 'localhost',
        int $port = 5432,
        ?string $user = null,
        #[SensitiveParameter]
        ?string $password = null,
        array $options = [],
    ): self {
        return new self(
            host: $host,
            port: $port,
            database: $database,
            user: $user,
            password: $password,
            options: $options,
        );
    }

    /**
     * Parses a libpq keyword/value connection string.
     *
     * @throws \InvalidArgumentException
     */
    public static function fromString(#[SensitiveParameter] string $connectionString): self
    {
        $parts = self::parseConnectionString($connectionString);

        if (!array_key_exists('dbname', $parts)) {
            throw new InvalidArgumentException('Missing dbname in connection string');
        }

        return new self(
            host: $parts['host'] ?? 'localhost',
            port: array_key_exists('port', $parts) ? (int) $parts['port'] : 5432,
            database: $parts['dbname'],
            user: $parts['user'] ?? null,
            password: $parts['password'] ?? null,
            options: array_diff_key($parts, array_flip(['host', 'port', 'dbname', 'user', 'password'])),
        );
    }

    /**
     * Mask password in debug output to prevent accidental exposure.
     *
     * @return array<string, mixed>
     */
    public function __debugInfo(): array
    {
        return [
            'host' => $this->host,
            'port' => $this->port,
            'database' => $this->database,
            'user' => $this->user,
            'password' => $this->password !== null ? '***' : null,
            'options' => $this->options,
        ];
    }

    public function database(): string
    {
        return $this->database;
    }

    public function host(): string
    {
        return $this->host;
    }

    /**
     * @return array<string, string>
     */
    public function options(): array
    {
        return $this->options;
    }

    public function password(): ?string
    {
        return $this->password;
    }

    public function port(): int
    {
        return $this->port;
    }

    /**
     * Convert connection parameters to a libpq connection string.
     */
    public function toString(): string
    {
        $parts = [
            sprintf('host=%s', self::connectionStringValue($this->host)),
            sprintf('port=%d', $this->port),
            sprintf('dbname=%s', self::connectionStringValue($this->database)),
        ];

        if ($this->user !== null) {
            $parts[] = sprintf('user=%s', self::connectionStringValue($this->user));
        }

        if ($this->password !== null) {
            $parts[] = sprintf('password=%s', self::connectionStringValue($this->password));
        }

        foreach ($this->options as $key => $value) {
            $parts[] = sprintf('%s=%s', $key, self::connectionStringValue($value));
        }

        return implode(' ', $parts);
    }

    public function user(): ?string
    {
        return $this->user;
    }

    public function withDatabase(string $database): self
    {
        return new self(
            host: $this->host,
            port: $this->port,
            database: $database,
            user: $this->user,
            password: $this->password,
            options: $this->options,
        );
    }

    public function withDatabaseSuffix(string $suffix): self
    {
        if ($suffix === '') {
            return $this;
        }

        return $this->withDatabase($this->database . $suffix);
    }

    public function withHost(string $host): self
    {
        return new self(
            host: $host,
            port: $this->port,
            database: $this->database,
            user: $this->user,
            password: $this->password,
            options: $this->options,
        );
    }

    public function withOption(string $key, string $value): self
    {
        return new self(
            host: $this->host,
            port: $this->port,
            database: $this->database,
            user: $this->user,
            password: $this->password,
            options: array_merge($this->options, [$key => $value]),
        );
    }

    /**
     * @param array<string, string> $options
     */
    public function withOptions(array $options): self
    {
        return new self(
            host: $this->host,
            port: $this->port,
            database: $this->database,
            user: $this->user,
            password: $this->password,
            options: $options,
        );
    }

    public function withPassword(#[SensitiveParameter] ?string $password): self
    {
        return new self(
            host: $this->host,
            port: $this->port,
            database: $this->database,
            user: $this->user,
            password: $password,
            options: $this->options,
        );
    }

    public function withPort(int $port): self
    {
        return new self(
            host: $this->host,
            port: $port,
            database: $this->database,
            user: $this->user,
            password: $this->password,
            options: $this->options,
        );
    }

    /**
     * @param array<string, string> $settings
     */
    public function withSessionSettings(array $settings): self
    {
        if ($settings === []) {
            return $this;
        }

        $pairs = [];

        if (array_key_exists('options', $this->options) && $this->options['options'] !== '') {
            $pairs[] = $this->options['options'];
        }

        foreach ($settings as $name => $value) {
            // @mago-expect analysis:redundant-cast -- PHP stores a numeric string key as an int
            $name = (string) $name;

            if (preg_match('/^[A-Za-z_][A-Za-z0-9_.]*$/', $name) !== 1) {
                throw new InvalidArgumentException(sprintf(
                    'Session setting name "%s" is invalid. A name starts with a letter or "_" and holds only letters, digits, "_" and ".".',
                    $name,
                ));
            }

            $pairs[] = sprintf('-c %s=%s', $name, preg_replace('/(\\\\|\s)/', '\\\\$1', $value));
        }

        return $this->withOption('options', implode(' ', $pairs));
    }

    public function withUser(?string $user): self
    {
        return new self(
            host: $this->host,
            port: $this->port,
            database: $this->database,
            user: $user,
            password: $this->password,
            options: $this->options,
        );
    }

    private static function connectionStringValue(string $value): string
    {
        if ($value !== '' && preg_match('/[\s\'\\\\]/', $value) !== 1) {
            return $value;
        }

        return "'" . str_replace(['\\', "'"], ['\\\\', "\\'"], $value) . "'";
    }

    /**
     * @return array<string, string>
     */
    private static function parseConnectionString(#[SensitiveParameter] string $connectionString): array
    {
        $parts = [];
        $length = strlen($connectionString);
        $position = 0;

        while ($position < $length) {
            if (ctype_space($connectionString[$position])) {
                $position++;

                continue;
            }

            $key = '';

            while (
                $position < $length
                && $connectionString[$position] !== '='
                && !ctype_space($connectionString[$position])
            ) {
                $key .= $connectionString[$position++];
            }

            while ($position < $length && ctype_space($connectionString[$position])) {
                $position++;
            }

            if ($position >= $length || $connectionString[$position] !== '=') {
                throw new InvalidArgumentException(sprintf('Missing "=" after "%s" in connection string', $key));
            }

            $position++;

            while ($position < $length && ctype_space($connectionString[$position])) {
                $position++;
            }

            $value = '';

            if ($position < $length && $connectionString[$position] === "'") {
                $position++;

                while (true) {
                    if ($position >= $length) {
                        throw new InvalidArgumentException('Unterminated quoted value in connection string');
                    }

                    if ($connectionString[$position] === "'") {
                        $position++;

                        break;
                    }

                    if ($connectionString[$position] === '\\' && ($position + 1) < $length) {
                        $position++;
                    }

                    $value .= $connectionString[$position++];
                }
            } else {
                while ($position < $length && !ctype_space($connectionString[$position])) {
                    if ($connectionString[$position] === '\\') {
                        $position++;

                        if ($position < $length) {
                            $value .= $connectionString[$position++];
                        }

                        continue;
                    }

                    $value .= $connectionString[$position++];
                }
            }

            $parts[$key] = $value;
        }

        return $parts;
    }
}
