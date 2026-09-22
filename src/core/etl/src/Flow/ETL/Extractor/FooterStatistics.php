<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;

use function max;
use function round;

final readonly class FooterStatistics
{
    /**
     * @var int<0, max>
     */
    public int $files;

    /**
     * @param Statistics $statistics what the read footers declare, merged
     * @param int $files how many footers were read
     */
    public function __construct(
        public Statistics $statistics,
        int $files,
    ) {
        if ($files < 0) {
            throw new InvalidArgumentException('Read footer count must not be negative, given: ' . $files);
        }

        $this->files = $files;
    }

    /**
     * Exact when every listed file's footer was read; otherwise the read footers are scaled to the listing
     * (DuckDB's rule, parquet_extension.cpp:866-877). The offset is taken off the rows, never below zero.
     */
    public function of(int $listedFiles, int $offset): Statistics
    {
        $rows = $this->statistics->rows->estimate ?? 0;
        $bytes = $this->statistics->size->estimate ?? 0;

        if ($this->files === 0 || $this->files >= $listedFiles) {
            return new Statistics(rows: Cardinality::exact(max(0, $rows - $offset)), size: Cardinality::exact($bytes));
        }

        return new Statistics(
            rows: Cardinality::approximately(
                max(0, (int) round(($rows * $listedFiles) / $this->files) - $offset),
                Cardinality::DEFAULT_RELATIVE_ERROR,
            ),
            size: Cardinality::approximately(
                (int) round(($bytes * $listedFiles) / $this->files),
                Cardinality::DEFAULT_RELATIVE_ERROR,
            ),
        );
    }
}
