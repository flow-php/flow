<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\SFTP\LiveSFTPClient;
use phpseclib4\Net\SFTP;

final class LiveSFTPClientTest extends FlowTestCase
{
    public function test_a_live_client_has_no_configuration(): void
    {
        $sftp = new SFTP('localhost');
        $source = new LiveSFTPClient($sftp);

        static::assertSame($sftp, $source->client());
        static::assertNull($source->normalize());
    }
}
