<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use function Flow\Filesystem\DSL\path;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;

final class Options
{
    private readonly BlockFactory $blockFactory;

    private int $partSize = 1024 * 1024 * 5;

    private readonly Path $tmpDir;

    public function __construct()
    {
        $this->blockFactory = new NativeLocalFileBlocksFactory();
        $this->tmpDir = path('aws-s3://_$azure_flow_tmp$/');
    }

    public function blockFactory() : BlockFactory
    {
        return $this->blockFactory;
    }

    public function partSize() : int
    {
        return $this->partSize;
    }

    public function tmpDir() : Path
    {
        return $this->tmpDir;
    }

    public function withBlockSize(int $bytes) : self
    {
        if ($bytes <= 1024 * 1024 * 5) {
            throw new InvalidArgumentException('Block size must be greater than 5Mb');
        }

        $this->partSize = $bytes;

        return $this;
    }
}
