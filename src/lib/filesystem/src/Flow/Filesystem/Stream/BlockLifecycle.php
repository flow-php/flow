<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

/**
 * A callback interface representing block of data lifecycle.
 * Methods of this interface are called by the Blocks collection.
 */
interface BlockLifecycle
{
    /**
     * Method called by the Blocks collection when the block is filled with data and
     * can be uploaded to the remote filesystem.
     *
     * If necessary after uploading a block, it's identifier should be appended to the BlockList
     * that at the end will be used to commit the block list into a file.
     */
    public function filled(Block $block) : void;
}
