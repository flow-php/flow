<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Inference;

interface Sniffs
{
    /**
     * Whole-unit sniffs for sampleSize(-1): units 0..$units-1, every unit sniffed with budget -1, Samples in unit order.
     *
     * @param int<0, max> $units
     *
     * @return null|list<Sample> null: the units are sniffed in this process
     */
    public function sniff(Sniffer $sniffer, int $units): ?array;
}
