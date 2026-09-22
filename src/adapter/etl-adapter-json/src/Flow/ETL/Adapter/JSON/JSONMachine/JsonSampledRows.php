<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Exception\InvalidArgumentException;

final readonly class JsonSampledRows
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
     * @param int $rows rows read while sampling
     * @param int $bytes bytes of the lines those rows came from, line endings and blank lines included; 0 for a document
     * @param bool $wholeFile the sample reached the end of the file, so $rows is every row it holds
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
