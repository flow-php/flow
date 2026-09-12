<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Double;

use Override;
use phpseclib3\Net\SFTP;

final class FixedListingSFTP extends SFTP
{
    /**
     * @param array<array-key, array<array-key, mixed>>|false $listing
     */
    public function __construct(
        private readonly array|false $listing,
    ) {
        parent::__construct('localhost');
    }

    #[Override]
    public function rawlist($dir = '.', $recursive = false)
    {
        return $this->listing;
    }
}
