<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

final readonly class ConnectionParameters
{
    private function __construct(
        public string $connectionString,
    ) {
    }

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
        #[\SensitiveParameter]
        ?string $password = null,
        array $options = [],
    ) : self {
        $parts = [
            \sprintf('host=%s', $host),
            \sprintf('port=%d', $port),
            \sprintf('dbname=%s', $database),
        ];

        if ($user !== null) {
            $parts[] = \sprintf('user=%s', $user);
        }

        if ($password !== null) {
            $parts[] = \sprintf('password=%s', $password);
        }

        foreach ($options as $key => $value) {
            $parts[] = \sprintf('%s=%s', $key, $value);
        }

        return new self(\implode(' ', $parts));
    }

    /**
     * Create from a PostgreSQL connection string.
     */
    public static function fromString(#[\SensitiveParameter] string $connectionString) : self
    {
        return new self($connectionString);
    }

    /**
     * Mask password in debug output to prevent accidental exposure.
     *
     * @return array<string, string>
     */
    public function __debugInfo() : array
    {
        return [
            'connectionString' => \preg_replace('/password=[^\s]+/', 'password=***', $this->connectionString) ?? $this->connectionString,
        ];
    }
}
