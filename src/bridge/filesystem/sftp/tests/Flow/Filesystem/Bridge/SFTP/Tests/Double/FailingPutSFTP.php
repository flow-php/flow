<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Double;

use Override;
use phpseclib3\Net\SFTP;

final class FailingPutSFTP extends SFTP
{
    #[Override]
    public function put(
        $remote_file,
        $data,
        $mode = self::SOURCE_STRING,
        $start = -1,
        $local_start = -1,
        $progressCallback = null,
    ) {
        return false;
    }
}
