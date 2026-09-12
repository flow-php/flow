<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Integration;

use Flow\Filesystem\Exception\RuntimeException;
use phpseclib3\Crypt\EC;

use function Flow\Filesystem\Bridge\SFTP\DSL\sftp_client;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class SFTPConnectionTest extends SFTPTestCase
{
    public function test_authenticated_client_sees_the_upload_directory(): void
    {
        static::assertTrue($this->sftpContext()->client()->is_dir('/upload'));
    }

    public function test_a_private_key_is_accepted_as_a_credential(): void
    {
        try {
            sftp_client(
                type_string()->assert($_ENV['SFTP_HOST']),
                type_string()->assert($_ENV['SFTP_USER']),
                EC::createKey('Ed25519'),
                type_integer()->cast($_ENV['SFTP_PORT']),
            );

            static::fail('Expected the server to reject a key it does not know');
        } catch (RuntimeException $e) {
            static::assertStringContainsString('Failed to authenticate', $e->getMessage());
        }
    }

    public function test_wrong_password_fails_without_leaking_the_credential(): void
    {
        try {
            sftp_client(
                type_string()->assert($_ENV['SFTP_HOST']),
                type_string()->assert($_ENV['SFTP_USER']),
                'definitely-not-the-password',
                type_integer()->cast($_ENV['SFTP_PORT']),
            );

            static::fail('Expected authentication against the SFTP server to fail');
        } catch (RuntimeException $e) {
            static::assertStringContainsString('Failed to authenticate', $e->getMessage());
            static::assertStringNotContainsString('definitely-not-the-password', $e->getMessage());
        }
    }
}
