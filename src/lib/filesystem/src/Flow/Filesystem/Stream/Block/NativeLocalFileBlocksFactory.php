<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream\Block;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\{Block, BlockFactory};

final readonly class NativeLocalFileBlocksFactory implements BlockFactory
{
    private string $blockLocation;

    /**
     * @param null|string $blockLocation - directory where blocks will be stored, defaults to system temp directory, if directory does not exist it will be created
     *
     * @throws InvalidArgumentException
     */
    public function __construct(?string $blockLocation = null)
    {
        if ($blockLocation) {
            if (!\file_exists($blockLocation) || !\is_dir($blockLocation)) {
                if (!\mkdir($blockLocation, 0777, true) && !\is_dir($blockLocation)) {
                    throw new InvalidArgumentException('Block location must be a valid directory, got: ' . $blockLocation);
                }
            }
        }

        $this->blockLocation = $blockLocation ?: \sys_get_temp_dir();
    }

    public function create(int $size) : Block
    {
        $id = \Flow\ETL\DSL\generate_random_string();

        return new Block($id, $size, new Path($this->blockLocation . DIRECTORY_SEPARATOR . $id));
    }
}
