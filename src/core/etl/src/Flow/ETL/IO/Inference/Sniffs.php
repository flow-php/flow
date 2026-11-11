<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Inference;

use Flow\ETL\Foundation\Progress;

interface Sniffs
{
    /**
     * Whole-unit sniffs for everyRow(): units 0..$units-1, each sniffed with a null budget, Samples in unit order.
     *
     * @param int<0, max> $units
     *
     * @return null|list<Sample> null: the units are sniffed in this process
     */
    public function sniff(Sniffer $sniffer, int $units, Progress $progress): ?array;
}
