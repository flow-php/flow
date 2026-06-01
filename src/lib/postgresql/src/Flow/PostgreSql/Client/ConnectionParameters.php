<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use InvalidArgumentException;
use SensitiveParameter;

use function array_diff_key;
use function array_flip;
use function array_merge;
use function implode;
use function preg_match_all;
use function sprintf;

use const PREG_SET_ORDER;

final readonly class ConnectionParameters
{
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
     * Create from a PostgreSQL libpq connection string.
     *
     * Parses connection strings in libpq format: key=value pairs separated by spaces.
     * Supports quoted values for values containing spaces (e.g., password='my secret').
     *
     * @throws \InvalidArgumentException if dbname is missing
     */
    public static function fromString(#[SensitiveParameter] string $connectionString): self
    {
        $parts = [];
        $matches = [];
        $pattern = '/(\w+)=(?:\'([^\']*)\'|([^\s]*))/';
        preg_match_all($pattern, $connectionString, $matches, PREG_SET_ORDER);

        foreach ($matches as $match) {
            $key = $match[1];
            $quotedValue = $match[2] ?? '';
            $unquotedValue = $match[3] ?? '';
            $value = $quotedValue !== '' ? $quotedValue : $unquotedValue;
            $parts[$key] = $value;
        }

        if (!isset($parts['dbname'])) {
            throw new InvalidArgumentException('Missing dbname in connection string');
        }

        return new self(
            host: $parts['host'] ?? 'localhost',
            port: isset($parts['port']) ? (int) $parts['port'] : 5432,
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
            sprintf('host=%s', $this->host),
            sprintf('port=%d', $this->port),
            sprintf('dbname=%s', $this->database),
        ];

        if ($this->user !== null) {
            $parts[] = sprintf('user=%s', $this->user);
        }

        if ($this->password !== null) {
            $parts[] = sprintf('password=%s', $this->password);
        }

        foreach ($this->options as $key => $value) {
            $parts[] = sprintf('%s=%s', $key, $value);
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
}
