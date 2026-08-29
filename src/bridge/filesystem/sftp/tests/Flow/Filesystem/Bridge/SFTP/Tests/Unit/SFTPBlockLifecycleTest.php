<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\SFTP\SFTPDestinationStream\SFTPBlockLifecycle;
use Flow\Filesystem\Bridge\SFTP\SFTPDestinationStream\WriteOffset;
use Flow\Filesystem\Bridge\SFTP\Tests\Double\FailingPutSFTP;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Stream\Block;

use function Flow\Filesystem\DSL\path;
use function sys_get_temp_dir;
use function uniqid;

final class SFTPBlockLifecycleTest extends FlowTestCase
{
    public function test_a_failed_upload_is_reported_as_a_flow_exception(): void
    {
        $block = new Block('block-01', 1024, path(sys_get_temp_dir() . '/flow_sftp_block_' . uniqid() . '.bin'));
        $block->append('some data');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('SFTP session is no longer usable, cannot upload a block of /upload/orders.csv');

        (new SFTPBlockLifecycle(
            new FailingPutSFTP('localhost'),
            path('sftp:///upload/orders.csv'),
            new WriteOffset(),
        ))->filled($block);
    }
}
