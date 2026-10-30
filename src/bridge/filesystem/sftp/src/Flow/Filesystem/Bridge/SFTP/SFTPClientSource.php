<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use phpseclib4\Net\SFTP;

/**
 * @import-type SFTPClientConfiguration from ConfiguredSFTPClient
 */
interface SFTPClientSource
{
    public function client(): SFTP;

    /**
     * @return null|SFTPClientConfiguration null for a live client that cannot be rebuilt elsewhere
     */
    public function normalize(): ?array;
}
