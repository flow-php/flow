<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\SFTPDestinationStream;

use Flow\Filesystem\Bridge\SFTP\RemotePath;
use Flow\Filesystem\Bridge\SFTP\SFTPSession;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Block;
use Flow\Filesystem\Stream\BlockLifecycle;
use phpseclib3\Net\SFTP;

use function unlink;

final class SFTPBlockLifecycle implements BlockLifecycle
{
    private const int TRUNCATE_REMOTE_FILE = -1;

    private bool $firstBlockUploaded = false;

    private readonly RemotePath $remotePath;

    private readonly SFTPSession $session;

    public function __construct(
        private readonly SFTP $sftp,
        Path $path,
        private readonly WriteOffset $offset,
        private readonly WriteMode $mode = WriteMode::BLANK,
    ) {
        $this->remotePath = RemotePath::from($path);
        $this->session = new SFTPSession($sftp);
    }

    public function filled(Block $block): void
    {
        $uploaded = $this->sftp->put(
            $this->remotePath->toString(),
            $block->path()->path(),
            SFTP::SOURCE_LOCAL_FILE,
            $this->startOffset(),
        );

        if ($uploaded === false) {
            $this->session->assertAlive('upload a block of ' . $this->remotePath->toString());

            throw new RuntimeException('Could not upload block to ' . $this->remotePath->toString());
        }

        unlink($block->path()->path());

        $this->firstBlockUploaded = true;
        $this->offset->advance($block->size());
    }

    private function startOffset(): int
    {
        if ($this->mode === WriteMode::BLANK && !$this->firstBlockUploaded) {
            return self::TRUNCATE_REMOTE_FILE;
        }

        return $this->offset->current();
    }
}
