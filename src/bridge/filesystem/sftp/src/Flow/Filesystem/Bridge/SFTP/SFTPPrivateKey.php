<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use Flow\Filesystem\Exception\InvalidArgumentException;
use phpseclib4\Crypt\Common\PrivateKey;
use phpseclib4\Crypt\PublicKeyLoader;
use phpseclib4\Net\SFTP;
use SensitiveParameter;

final readonly class SFTPPrivateKey implements SFTPCredential
{
    public function __construct(
        #[SensitiveParameter]
        private string $pem,
        #[SensitiveParameter]
        private ?string $passphrase = null,
    ) {}

    /**
     * @throws InvalidArgumentException when the PEM is not a private key
     */
    public function login(SFTP $sftp, string $user): bool
    {
        $key = PublicKeyLoader::load($this->pem, $this->passphrase);

        if (!$key instanceof PrivateKey) {
            throw new InvalidArgumentException('SFTP private key PEM does not hold a private key');
        }

        return $sftp->login($user, $key);
    }

    /**
     * @return array{private_key: string, passphrase: null|string}
     */
    public function normalize(): array
    {
        return ['private_key' => $this->pem, 'passphrase' => $this->passphrase];
    }
}
