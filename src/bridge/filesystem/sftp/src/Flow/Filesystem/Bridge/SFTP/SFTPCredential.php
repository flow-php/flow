<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use phpseclib4\Net\SFTP;

interface SFTPCredential
{
    public function login(SFTP $sftp, string $user): bool;

    /**
     * @return array{password: string}|array{private_key: string, passphrase: null|string}
     */
    public function normalize(): array;
}
