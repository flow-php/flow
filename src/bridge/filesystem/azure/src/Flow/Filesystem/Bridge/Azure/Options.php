<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure;

use Flow\Azure\SDK\BlobService\ListBlobs\{ListBlobOptions, OptionInclude, OptionShowOnly};
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;

final class Options
{
    private BlockFactory $blockFactory;

    private int $blockSize = 1024 * 1024 * 4;

    /**
     * @var null|array<OptionInclude>
     */
    private ?array $listBlobInclude = null;

    private ?int $listBlobMaxResults = null;

    private ?OptionShowOnly $listBlobShowOnly = null;

    public function __construct()
    {
        $this->blockFactory = new NativeLocalFileBlocksFactory();
    }

    public function blockFactory() : BlockFactory
    {
        return $this->blockFactory;
    }

    public function blockSize() : int
    {
        return $this->blockSize;
    }

    public function listBlobOptions() : ListBlobOptions
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

    public function withBlockFactory(BlockFactory $blockFactory) : self
    {
        $this->blockFactory = $blockFactory;

        return $this;
    }

    public function withBlockSize(int $blockSize) : self
    {
        $this->blockSize = $blockSize;

        return $this;
    }

    public function withListBlobInclude(OptionInclude ...$listBlobInclude) : self
    {
        $this->listBlobInclude = $listBlobInclude;

        return $this;
    }

    public function withListBlobMaxResults(int $listBlobMaxResults) : self
    {
        $this->listBlobMaxResults = $listBlobMaxResults;

        return $this;
    }

    public function withListBlobShowOnly(OptionShowOnly $listBlobShowOnly) : self
    {
        $this->listBlobShowOnly = $listBlobShowOnly;

        return $this;
    }
}
