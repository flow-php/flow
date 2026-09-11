<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream\Block;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Stream\Block;
use Flow\Filesystem\Stream\BlockFactory;

use function bin2hex;
use function file_exists;
use function Flow\Filesystem\DSL\path;
use function is_dir;
use function mkdir;
use function random_bytes;
use function sys_get_temp_dir;

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
            if (!file_exists($blockLocation) || !is_dir($blockLocation)) {
                if (!mkdir($blockLocation, 0777, true) && !is_dir($blockLocation)) {
                    throw new InvalidArgumentException('Block location must be a valid directory, got: '
                    . $blockLocation);
                }
            }
        }

        $this->blockLocation = $blockLocation ?: sys_get_temp_dir();
    }

    public function create(int $size): Block
    {
        $id = bin2hex(random_bytes(16));

        return new Block($id, $size, path($this->blockLocation . DIRECTORY_SEPARATOR . $id));
    }
}
