<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure;

use Flow\Azure\SDK\BlobService\ListBlobs\ListBlobOptions;
use Flow\Azure\SDK\BlobService\ListBlobs\OptionInclude;
use Flow\Azure\SDK\BlobService\ListBlobs\OptionShowOnly;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;

final class Options
{
    private BlockFactory $blockFactory;

    /** @var int<1, max> */
    private int $blockSize = 1024 * 1024 * 4;

    private bool $fileFastPath = true;

    /**
     * @var null|array<OptionInclude>
     */
    private ?array $listBlobInclude = null;

    private ?int $listBlobMaxResults = null;

    private ?OptionShowOnly $listBlobShowOnly = null;

    private Path $tmpDir;

    public function __construct()
    {
        $this->blockFactory = new NativeLocalFileBlocksFactory();
        $this->tmpDir = \Flow\Filesystem\DSL\path('azure-blob://_$azure_flow_tmp$/');
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

    public function fileFastPath(): bool
    {
        return $this->fileFastPath;
    }

    public function listBlobOptions(): ListBlobOptions
    {
        $listBlobOptions = new ListBlobOptions();

        if ($this->listBlobInclude !== null) {
            $listBlobOptions->withInclude(...$this->listBlobInclude);
        }

        if ($this->listBlobMaxResults !== null) {
            $listBlobOptions->withMaxResults($this->listBlobMaxResults);
        }

        if ($this->listBlobShowOnly !== null) {
            $listBlobOptions->withShowOnly($this->listBlobShowOnly);
        }

        return $listBlobOptions;
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

    public function withBlockSize(int $blockSize): self
    {
        if ($blockSize < 1) {
            throw new InvalidArgumentException('Block size must be greater than 0');
        }

        $this->blockSize = $blockSize;

        return $this;
    }

    /**
     * When enabled (default), list() on a single-file path will first attempt getBlobProperties on the blob.
     * If it exists, list() yields just that single FileStatus and never issues listBlobs.
     * This avoids the container-list permission requirement and an extra round-trip when reading single files.
     * Disable if your workloads typically pass folder/prefix paths to list(), to skip the (failing) properties call.
     */
    public function withFileFastPath(bool $enabled = true): self
    {
        $this->fileFastPath = $enabled;

        return $this;
    }

    public function withListBlobInclude(OptionInclude ...$listBlobInclude): self
    {
        $this->listBlobInclude = $listBlobInclude;

        return $this;
    }

    public function withListBlobMaxResults(int $listBlobMaxResults): self
    {
        $this->listBlobMaxResults = $listBlobMaxResults;

        return $this;
    }

    public function withListBlobShowOnly(OptionShowOnly $listBlobShowOnly): self
    {
        $this->listBlobShowOnly = $listBlobShowOnly;

        return $this;
    }

    public function withTmpDir(Path $tmpDir): self
    {
        $this->tmpDir = $tmpDir;

        return $this;
    }
}
