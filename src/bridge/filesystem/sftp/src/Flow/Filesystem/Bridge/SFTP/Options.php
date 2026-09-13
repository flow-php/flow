<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;
use Flow\Filesystem\SizeUnits;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;

use function Flow\Filesystem\DSL\path;

final class Options
{
    /** @var int<1, max> */
    public const int DEFAULT_BLOCK_SIZE = 16 * SizeUnits::MiB_SIZE;

    /** @var int<1, max> */
    public const int DEFAULT_READ_CHUNK_SIZE = SizeUnits::MiB_SIZE;

    private BlockFactory $blockFactory;

    /** @var int<1, max> */
    private int $blockSize = self::DEFAULT_BLOCK_SIZE;

    /** @var int<1, max> */
    private int $readChunkSize = self::DEFAULT_READ_CHUNK_SIZE;

    private Path $tmpDir;

    public function __construct()
    {
        $this->blockFactory = new NativeLocalFileBlocksFactory();
        $this->tmpDir = path('sftp://_$flow_tmp$/');
    }

    public function blockFactory(): BlockFactory
    {
        return $this->blockFactory;
    }

    /**
     * @return int<1, max>
     */
    public function blockSize(): int
    {
        return $this->blockSize;
    }

    /**
     * @return int<1, max>
     */
    public function readChunkSize(): int
    {
        return $this->readChunkSize;
    }

    public function tmpDir(): Path
    {
        return $this->tmpDir;
    }

    public function withBlockFactory(BlockFactory $blockFactory): self
    {
        $this->blockFactory = $blockFactory;

        return $this;
    }

    public function withBlockSize(int $bytes): self
    {
        if ($bytes < 1) {
            throw new InvalidArgumentException('Block size must be greater than 0, given: ' . $bytes);
        }

        $this->blockSize = $bytes;

        return $this;
    }

    public function withReadChunkSize(int $bytes): self
    {
        if ($bytes < 1) {
            throw new InvalidArgumentException('Read chunk size must be greater than 0, given: ' . $bytes);
        }

        $this->readChunkSize = $bytes;

        return $this;
    }

    public function withTmpDir(Path $tmpDir): self
    {
        $this->tmpDir = $tmpDir;

        return $this;
    }
}
