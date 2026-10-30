<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\SFTP\SFTPPrivateKey;
use Flow\Filesystem\Exception\InvalidArgumentException;
use phpseclib4\Crypt\RSA;
use phpseclib4\Net\SFTP;

final class SFTPPrivateKeyTest extends FlowTestCase
{
    public function test_normalize_carries_the_pem_and_the_passphrase(): void
    {
        static::assertSame(['private_key' => 'pem', 'passphrase' => null], (new SFTPPrivateKey('pem'))->normalize());
        static::assertSame(
            ['private_key' => 'pem', 'passphrase' => 'p'],
            (new SFTPPrivateKey('pem', 'p'))->normalize(),
        );
    }

    public function test_a_pem_holding_a_public_key_is_refused_at_login(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('SFTP private key PEM does not hold a private key');

        (new SFTPPrivateKey(RSA::createKey(1024)->getPublicKey()->toString('PKCS8')))->login(
            new SFTP('localhost'),
            'bob',
        );
    }
}
