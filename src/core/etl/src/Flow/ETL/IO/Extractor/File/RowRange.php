<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

final readonly class RowRange
{
    /**
     * @var int<0, max>
     */
    public int $count;

    /**
     * @var int<0, max>
     */
    public int $offset;

    public function __construct(int $offset, int $count)
    {
        if ($offset < 0) {
            throw new InvalidArgumentException('Row range offset must not be negative, given: ' . $offset);
        }

        if ($count < 0) {
            throw new InvalidArgumentException('Row range count must not be negative, given: ' . $count);
        }

        $this->offset = $offset;
        $this->count = $count;
    }
}
