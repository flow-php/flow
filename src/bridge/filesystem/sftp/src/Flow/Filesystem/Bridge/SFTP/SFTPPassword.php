<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use phpseclib4\Net\SFTP;
use SensitiveParameter;

final readonly class SFTPPassword implements SFTPCredential
{
    public function __construct(
        #[SensitiveParameter]
        private string $password,
    ) {}

    public function login(SFTP $sftp, string $user): bool
    {
        return $sftp->login($user, $this->password);
    }

    /**
     * @return array{password: string}
     */
    public function normalize(): array
    {
        return ['password' => $this->password];
    }
}
