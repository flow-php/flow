<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\Statistics;

use function is_infinite;
use function number_format;
use function round;

final readonly class StatisticsLine
{
    /**
     * Null when the source declares neither fact.
     */
    public function of(Statistics $statistics): ?string
    {
        if ($statistics->rows->isUnknown() && $statistics->size->isUnknown()) {
            return null;
        }

        return (
            'rows ' . $this->cardinality($statistics->rows) . ' · size ' . $this->cardinality($statistics->size, ' B')
        );
    }

    public function cardinality(Cardinality $cardinality, string $unit = ''): string
    {
        $atMost = $cardinality->atMost;
        $estimate = $cardinality->estimate;
        $exactly = $cardinality->exactly();

        if ($exactly !== null) {
            return 'exact ' . $this->number($exactly) . $unit;
        }

        if ($estimate === null) {
            return $atMost === null ? 'unknown' : '≤ ' . $this->number($atMost) . $unit;
        }

        $approximately = '~' . $this->number($estimate) . $unit . ' ±' . $this->percent($cardinality->relativeError);

        return $atMost === null ? $approximately : $approximately . ' (≤ ' . $this->number($atMost) . $unit . ')';
    }

    public function percent(?float $error): string
    {
        return match (true) {
            $error === null => 'unknown',
            is_infinite($error) => '∞',
            default => round($error * 100, 2) . '%',
        };
    }

    public function number(int $number): string
    {
        return number_format($number, 0, '', ' ');
    }
}
