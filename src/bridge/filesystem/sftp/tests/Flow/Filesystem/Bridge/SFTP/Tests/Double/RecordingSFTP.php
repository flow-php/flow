<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Double;

use Override;
use phpseclib3\Net\SFTP;

final class RecordingSFTP extends SFTP
{
    /** @var array<int, string> */
    public array $listedDirectories = [];

    #[Override]
    public function rawlist($dir = '.', $recursive = false)
    {
        $this->listedDirectories[] = $dir;

        return parent::rawlist($dir, $recursive);
    }
}
