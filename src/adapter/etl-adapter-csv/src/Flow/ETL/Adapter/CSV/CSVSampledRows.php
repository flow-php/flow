<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\InvalidArgumentException;

final readonly class CSVSampledRows
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
     * @param int $rows rows read while sniffing
     * @param int $bytes bytes those rows took as read, line endings included
     * @param bool $wholeFile the sniff reached the end of the file, so $rows is every row it holds
     */
    public function __construct(
        int $rows,
        int $bytes,
        public bool $wholeFile,
    ) {
        if ($rows < 0 || $bytes < 0) {
            throw new InvalidArgumentException(
                'Sampled rows and bytes must not be negative, given: ' . $rows . ' rows, ' . $bytes . ' bytes',
            );
        }

        $this->rows = $rows;
        $this->bytes = $bytes;
    }
}
