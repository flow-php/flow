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
     * @param bool $complete every read of the source ran to its end with no limit or partition filter pushed into it
     */
    public function __construct(
        public string $extractor,
        public Statistics $declared,
        public int $rows,
        public bool $complete,
    ) {}

    /**
     * How far the declared estimate was off, relative to the rows measured. Null without an estimate: an upper bound
     * alone is a guarantee, not a guess. Null for an incomplete read too: the declaration describes the whole source,
     * the rows measured only part of it. Against 0 measured rows any positive estimate is infinitely off.
     */
    public function rowsError(): ?float
    {
        $estimate = $this->declared->rows->estimate;

        if ($estimate === null || !$this->complete) {
            return null;
        }

        if ($this->rows === 0) {
            return $estimate === 0 ? 0.0 : INF;
        }

        return abs($estimate - $this->rows) / $this->rows;
    }
}
