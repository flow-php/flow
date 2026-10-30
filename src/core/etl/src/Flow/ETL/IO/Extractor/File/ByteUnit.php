<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function intdiv;

final readonly class ByteUnit
{
    /**
     * @var int<0, max>
     */
    public int $bytes;

    /**
     * @var int<0, max>
     */
    public int $rows;

    /**
     * @var int<0, max>
     */
    public int $start;

    public function __construct(int $start, int $bytes, int $rows)
    {
        if ($start < 0) {
            throw new InvalidArgumentException('Byte unit start must not be negative, given: ' . $start);
        }

        if ($bytes < 0) {
            throw new InvalidArgumentException('Byte unit size must not be negative, given: ' . $bytes);
        }

        if ($rows < 0) {
            throw new InvalidArgumentException('Byte unit row count must not be negative, given: ' . $rows);
        }

        $this->start = $start;
        $this->bytes = $bytes;
        $this->rows = $rows;
    }

    public function midpoint(): int
    {
        return $this->start + intdiv($this->bytes, 2);
    }
}
