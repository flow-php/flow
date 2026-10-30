<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Cardinality;

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
     * (DuckDB's rule, parquet_extension.cpp:866-877).
     */
    public function of(int $listedFiles): Statistics
    {
        $rows = $this->statistics->rows->estimate ?? 0;
        $bytes = $this->statistics->size->estimate ?? 0;

        if ($this->files === 0 || $this->files >= $listedFiles) {
            return new Statistics(rows: Cardinality::exact($rows), size: Cardinality::exact($bytes));
        }

        return new Statistics(
            rows: Cardinality::approximately(
                (int) round(($rows * $listedFiles) / $this->files),
                Cardinality::DEFAULT_RELATIVE_ERROR,
            ),
            size: Cardinality::approximately(
                (int) round(($bytes * $listedFiles) / $this->files),
                Cardinality::DEFAULT_RELATIVE_ERROR,
            ),
        );
    }
}
