<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;

use function Flow\Filesystem\DSL\path;

final class Options
{
    private readonly BlockFactory $blockFactory;

    private bool $fileFastPath = true;

    private int $partSize = 1024 * 1024 * 5;

    private readonly Path $tmpDir;

    public function __construct()
    {
        $this->blockFactory = new NativeLocalFileBlocksFactory();
        $this->tmpDir = path('aws-s3://_$azure_flow_tmp$/');
    }

    public function blockFactory(): BlockFactory
    {
        return $this->blockFactory;
    }

    public function fileFastPath(): bool
    {
        return $this->fileFastPath;
    }

    public function partSize(): int
    {
        return $this->partSize;
    }

    public function tmpDir(): Path
    {
        return $this->tmpDir;
    }

    public function withBlockSize(int $bytes): self
    {
        if ($bytes <= (1024 * 1024 * 5)) {
            throw new InvalidArgumentException('Block size must be greater than 5Mb');
        }

        $this->partSize = $bytes;

        return $this;
    }

    /**
     * When enabled (default), list() on a single-file path will first attempt a HEAD on the object.
     * If the object exists, list() yields just that single FileStatus and never issues listObjectsV2.
     * This avoids the s3:ListBucket permission requirement and an extra round-trip when reading single files.
     * Disable if your workloads typically pass folder/prefix paths to list(), to skip the (failing) HEAD.
     */
    public function withFileFastPath(bool $enabled = true): self
    {
        $this->fileFastPath = $enabled;

        return $this;
    }
}
