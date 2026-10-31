<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Metrics;

use Flow\ETL\IO\Extractor\Statistics;

use function abs;
use function is_finite;

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
     * rows_error: rowsError() when it is a finite number; null without an estimate, for an incomplete read, or when
     * the estimate was positive against 0 rows measured (infinitely off).
     *
     * @return array{extractor: string, declared: array{rows: array<string, null|float|int>, size: array<string, null|float|int>}, rows: int, complete: bool, rows_error: null|float}
     */
    public function normalize(): array
    {
        $rowsError = $this->rowsError();

        return [
            'extractor' => $this->extractor,
            'declared' => ['rows' => $this->declared->rows->normalize(), 'size' => $this->declared->size->normalize()],
            'rows' => $this->rows,
            'complete' => $this->complete,
            'rows_error' => $rowsError !== null && is_finite($rowsError) ? $rowsError : null,
        ];
    }

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
