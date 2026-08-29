<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\SFTP\SFTPSession;
use Flow\Filesystem\Exception\RuntimeException;
use phpseclib3\Net\SFTP;

final class SFTPSessionTest extends FlowTestCase
{
    public function test_asserting_a_dead_session_names_the_operation_that_failed(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('SFTP session is no longer usable, cannot read /upload/orders.csv');

        (new SFTPSession(new SFTP('localhost')))->assertAlive('read /upload/orders.csv');
    }
}
