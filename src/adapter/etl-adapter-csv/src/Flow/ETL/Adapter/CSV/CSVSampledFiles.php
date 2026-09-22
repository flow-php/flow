<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Cardinality;

use function round;

final readonly class CSVSampledFiles
{
    /**
     * @param int $files files the samples read
     * @param int $rows rows those files yielded
     * @param int $bytes bytes those rows took as read, line endings included
     * @param bool $whole every sampled file was read to its end
     */
    public function __construct(
        public int $files,
        public int $rows,
        public int $bytes,
        public bool $whole,
    ) {}

    /**
     * Listed bytes over the mean bytes of the sampled rows; exact when the samples read every row of every listed file.
     */
    public function estimatedRows(int $listedFiles, Cardinality $listedBytes): Cardinality
    {
        if ($this->whole && $this->files === $listedFiles) {
            return Cardinality::exact($this->rows);
        }

        if ($listedBytes->estimate === null || $this->rows === 0 || $this->bytes === 0) {
            return Cardinality::unknown();
        }

        return Cardinality::approximately(
            (int) round($listedBytes->estimate / ($this->bytes / $this->rows)),
            Cardinality::DEFAULT_RELATIVE_ERROR,
        );
    }
}
