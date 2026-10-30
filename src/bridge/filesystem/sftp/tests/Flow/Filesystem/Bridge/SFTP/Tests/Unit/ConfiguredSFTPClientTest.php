<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\SFTP\ConfiguredSFTPClient;
use Flow\Filesystem\Bridge\SFTP\SFTPPassword;
use Flow\Filesystem\Bridge\SFTP\SFTPPrivateKey;
use Flow\Filesystem\Exception\InvalidArgumentException;

final class ConfiguredSFTPClientTest extends FlowTestCase
{
    public function test_a_password_configuration_round_trips(): void
    {
        $client = new ConfiguredSFTPClient('sftp.test', 'bob', new SFTPPassword('secret'), 2222);

        static::assertSame(
            ['host' => 'sftp.test', 'user' => 'bob', 'port' => 2222, 'password' => 'secret'],
            $client->normalize(),
        );
        static::assertEquals($client, ConfiguredSFTPClient::fromArray($client->normalize()));
    }

    public function test_a_private_key_configuration_round_trips(): void
    {
        $client = new ConfiguredSFTPClient(
            'sftp.test',
            'bob',
            new SFTPPrivateKey('-----BEGIN PRIVATE KEY-----', 'phrase'),
        );

        static::assertSame(
            [
                'host' => 'sftp.test',
                'user' => 'bob',
                'port' => 22,
                'private_key' => '-----BEGIN PRIVATE KEY-----',
                'passphrase' => 'phrase',
            ],
            $client->normalize(),
        );
        static::assertEquals($client, ConfiguredSFTPClient::fromArray($client->normalize()));
    }

    public function test_the_port_defaults_to_22(): void
    {
        static::assertSame(
            22,
            ConfiguredSFTPClient::fromArray(['host' => 'h', 'user' => 'u', 'password' => 'p'])->normalize()['port'],
        );
    }

    public function test_both_credentials_are_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('SFTP client configuration must hold exactly one of "password" or "private_key"');

        ConfiguredSFTPClient::fromArray(['host' => 'h', 'user' => 'u', 'password' => 'p', 'private_key' => 'k']);
    }

    public function test_no_credential_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ConfiguredSFTPClient::fromArray(['host' => 'h', 'user' => 'u']);
    }

    public function test_a_passphrase_with_a_password_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('SFTP client "passphrase" belongs to a "private_key"');

        ConfiguredSFTPClient::fromArray(['host' => 'h', 'user' => 'u', 'password' => 'p', 'passphrase' => 'x']);
    }

    public function test_an_unknown_key_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('SFTP client configuration holds unknown keys: fingerprint');

        ConfiguredSFTPClient::fromArray(['host' => 'h', 'user' => 'u', 'password' => 'p', 'fingerprint' => 'x']);
    }

    public function test_an_empty_host_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('SFTP host can not be empty');

        new ConfiguredSFTPClient('', 'u', new SFTPPassword('p'));
    }

    public function test_a_port_out_of_range_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('SFTP port must be between 1 and 65535, given: 70000');

        new ConfiguredSFTPClient('h', 'u', new SFTPPassword('p'), 70000);
    }
}
