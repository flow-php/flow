<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\SFTPDestinationStream;

use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Block;
use Flow\Filesystem\Stream\BlockLifecycle;
use phpseclib4\Exception\BaseException;
use phpseclib4\Exception\FileSystemException;
use phpseclib4\Net\SFTP;

use function unlink;

final class SFTPBlockLifecycle implements BlockLifecycle
{
    private const int TRUNCATE_REMOTE_FILE = -1;

    private bool $firstBlockUploaded = false;

    private readonly string $remotePath;

    public function __construct(
        private readonly SFTP $sftp,
        public Path $path,
        private readonly WriteOffset $offset,
        private readonly WriteMode $mode = WriteMode::BLANK,
    ) {
        $this->remotePath = $path->path();
    }

    public function filled(Block $block): void
    {
        try {
            $this->sftp->put($this->remotePath, $block->path()->path(), SFTP::SOURCE_LOCAL_FILE, $this->startOffset());
        } catch (FileSystemException $e) {
            throw new RuntimeException('Could not upload block to ' . $this->remotePath, previous: $e);
        } catch (BaseException) {
            throw new RuntimeException('SFTP session is no longer usable, cannot upload a block of '
            . $this->remotePath);
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
