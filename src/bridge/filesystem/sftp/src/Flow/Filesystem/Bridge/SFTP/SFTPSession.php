<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use Flow\Filesystem\Exception\RuntimeException;
use phpseclib3\Net\SFTP;

use function sprintf;

final readonly class SFTPSession
{
    public function __construct(
        private SFTP $sftp,
    ) {}

    public function assertAlive(string $operation): void
    {
        if ($this->sftp->isConnected() && $this->sftp->isAuthenticated()) {
            return;
        }

        throw new RuntimeException(sprintf(
            'SFTP session is no longer usable, cannot %s. The connection was closed or lost, '
            . 'this bridge does not reconnect on your behalf.',
            $operation,
        ));
    }
}
