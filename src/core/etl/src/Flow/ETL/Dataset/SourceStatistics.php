<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset;

use Flow\ETL\Extractor\Statistics;

use function abs;

use const INF;

final readonly class SourceStatistics
{
    /**
     * @param string $extractor the source's short class name
     */
    public function __construct(
        public string $extractor,
        public Statistics $declared,
        public int $rows,
    ) {}

    /**
     * How far the declared estimate was off, relative to the rows measured. Null without an estimate: an upper bound
     * alone is a guarantee, not a guess. Against 0 measured rows any positive estimate is infinitely off.
     */
    public function rowsError(): ?float
    {
        $estimate = $this->declared->rows->estimate;

        if ($estimate === null) {
            return null;
        }

        if ($this->rows === 0) {
            return $estimate === 0 ? 0.0 : INF;
        }

        return abs($estimate - $this->rows) / $this->rows;
    }
}
