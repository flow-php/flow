<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use Flow\Filesystem\Bridge\SFTP\SFTPDestinationStream\SFTPBlockLifecycle;
use Flow\Filesystem\Bridge\SFTP\SFTPDestinationStream\WriteMode;
use Flow\Filesystem\Bridge\SFTP\SFTPDestinationStream\WriteOffset;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;
use Flow\Filesystem\Stream\Blocks;
use phpseclib3\Net\SFTP;

use function file_exists;
use function gettype;
use function is_resource;
use function rewind;
use function stream_get_meta_data;
use function unlink;

final class SFTPDestinationStream implements DestinationStream
{
    private bool $closed = false;

    private readonly RemotePath $remotePath;

    private readonly SFTPSession $session;

    public function __construct(
        private readonly Path $path,
        private readonly Blocks $blocks,
        private readonly SFTP $sftp,
        private readonly WriteMode $mode,
    ) {
        $this->remotePath = RemotePath::from($path);
        $this->session = new SFTPSession($sftp);
    }

    /**
     * @param int<1, max> $blockSize
     */
    public static function openAppend(
        SFTP $sftp,
        Path $path,
        BlockFactory $blockFactory = new NativeLocalFileBlocksFactory(),
        int $blockSize = Options::DEFAULT_BLOCK_SIZE,
    ): self {
        $remotePath = RemotePath::from($path)->toString();
        $offset = new WriteOffset($sftp->is_file($remotePath) ? (int) $sftp->filesize($remotePath) : 0);

        return new self(
            $path,
            new Blocks($blockSize, $blockFactory, new SFTPBlockLifecycle($sftp, $path, $offset, WriteMode::APPEND)),
            $sftp,
            WriteMode::APPEND,
        );
    }

    /**
     * @param int<1, max> $blockSize
     */
    public static function openBlank(
        SFTP $sftp,
        Path $path,
        BlockFactory $blockFactory = new NativeLocalFileBlocksFactory(),
        int $blockSize = Options::DEFAULT_BLOCK_SIZE,
    ): self {
        return new self(
            $path,
            new Blocks(
                $blockSize,
                $blockFactory,
                new SFTPBlockLifecycle($sftp, $path, new WriteOffset(), WriteMode::BLANK),
            ),
            $sftp,
            WriteMode::BLANK,
        );
    }

    public function append(string $data): DestinationStream
    {
        $this->blocks->append($data);

        return $this;
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->blocks->done();

        if ($this->blocks->size() === 0) {
            $this->discardUnusedBlock();
            $this->createEmptyFile();
        }

        $this->closed = true;
    }

    public function fromResource($resource): DestinationStream
    {
        if (!is_resource($resource)) {
            throw new InvalidArgumentException(
                'DestinationStream::fromResource expects resource type, given: ' . gettype($resource),
            );
        }

        if (stream_get_meta_data($resource)['seekable']) {
            rewind($resource);
        }

        $this->blocks->fromResource($resource);

        return $this;
    }

    public function isOpen(): bool
    {
        return !$this->closed;
    }

    public function path(): Path
    {
        return $this->path;
    }

    private function createEmptyFile(): void
    {
        if ($this->mode === WriteMode::APPEND && $this->sftp->is_file($this->remotePath->toString())) {
            return;
        }

        if ($this->sftp->put($this->remotePath->toString(), '', SFTP::SOURCE_STRING) === false) {
            $this->session->assertAlive('create ' . $this->remotePath->toString());

            throw new RuntimeException('Could not create empty file: ' . $this->remotePath->toString());
        }
    }

    private function discardUnusedBlock(): void
    {
        $blockPath = $this->blocks->block()->path()->path();

        if (file_exists($blockPath)) {
            unlink($blockPath);
        }
    }
}
