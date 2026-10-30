<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\RuntimeException;
use phpseclib4\Net\SFTP;
use SensitiveParameter;

use function array_diff;
use function array_key_exists;
use function array_keys;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function implode;
use function sprintf;

/**
 * @type SFTPClientConfiguration = array{host: string, user: string, port: int, password?: string, private_key?: string, passphrase?: null|string}
 */
final class ConfiguredSFTPClient implements SFTPClientSource
{
    private ?SFTP $client = null;

    public function __construct(
        private readonly string $host,
        private readonly string $user,
        private readonly SFTPCredential $credential,
        private readonly int $port = 22,
    ) {
        if ($host === '') {
            throw new InvalidArgumentException('SFTP host can not be empty');
        }

        if ($port < 1 || $port > 65535) {
            throw new InvalidArgumentException('SFTP port must be between 1 and 65535, given: ' . $port);
        }
    }

    /**
     * @param array<array-key, mixed> $config host, user, port? and exactly one of password | private_key (+ passphrase?)
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(#[SensitiveParameter] array $config): self
    {
        $unknown = array_diff(array_keys($config), ['host', 'user', 'port', 'password', 'private_key', 'passphrase']);

        if ($unknown !== []) {
            throw new InvalidArgumentException(
                'SFTP client configuration holds unknown keys: ' . implode(', ', $unknown),
            );
        }

        $host = type_string()->assert($config['host'] ?? null);
        $user = type_string()->assert($config['user'] ?? null);
        $port = type_integer()->assert($config['port'] ?? 22);
        $credentials =
            (array_key_exists('password', $config) ? 1 : 0) + (array_key_exists('private_key', $config) ? 1 : 0);

        if ($credentials !== 1) {
            throw new InvalidArgumentException(
                'SFTP client configuration must hold exactly one of "password" or "private_key"',
            );
        }

        if (array_key_exists('password', $config)) {
            if (array_key_exists('passphrase', $config)) {
                throw new InvalidArgumentException('SFTP client "passphrase" belongs to a "private_key"');
            }

            return new self($host, $user, new SFTPPassword(type_string()->assert($config['password'])), $port);
        }

        return new self(
            $host,
            $user,
            new SFTPPrivateKey(
                type_string()->assert($config['private_key']),
                type_optional(type_string())->assert($config['passphrase'] ?? null),
            ),
            $port,
        );
    }

    /**
     * @throws RuntimeException when the login fails
     */
    public function client(): SFTP
    {
        if ($this->client !== null) {
            return $this->client;
        }

        $sftp = new SFTP($this->host, $this->port);

        if (!$this->credential->login($sftp, $this->user)) {
            throw new RuntimeException(sprintf(
                'Failed to authenticate user "%s" against sftp://%s:%d',
                $this->user,
                $this->host,
                $this->port,
            ));
        }

        return $this->client = $sftp;
    }

    /**
     * @return SFTPClientConfiguration
     */
    public function normalize(): array
    {
        return ['host' => $this->host, 'user' => $this->user, 'port' => $this->port] + $this->credential->normalize();
    }
}
