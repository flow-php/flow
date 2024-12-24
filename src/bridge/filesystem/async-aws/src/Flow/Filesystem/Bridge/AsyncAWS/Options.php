<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use function Flow\Filesystem\DSL\path;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;

final class Options
{
    private BlockFactory $blockFactory;

    private int $partSize = 1024 * 1024 * 4;

    private Path $tmpDir;

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
}
