<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Exception\InvalidArgumentException;

use function Flow\Filesystem\Bridge\SFTP\DSL\sftp_filesystem_options;

final class OptionsTest extends FlowTestCase
{
    public function test_block_size_below_one_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Block size must be greater than 0, given: 0');

        sftp_filesystem_options()->withBlockSize(0);
    }

    public function test_read_chunk_size_below_one_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Read chunk size must be greater than 0, given: -1');

        sftp_filesystem_options()->withReadChunkSize(-1);
    }
}
