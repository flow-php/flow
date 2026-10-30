<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use phpseclib4\Net\SFTP;

final readonly class LiveSFTPClient implements SFTPClientSource
{
    public function __construct(
        private SFTP $sftp,
    ) {}

    public function client(): SFTP
    {
        return $this->sftp;
    }

    public function normalize(): ?array
    {
        return null;
    }
}
