<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Inference;

interface Sniffer
{
    /**
     * @return int<0, max> on-disk bytes of the unit; 0 when unknown or in memory
     */
    public function bytes(int $unit): int;

    /**
     * Releases what the sniffs left open.
     */
    public function close(): void;

    /**
     * @return list<string> columns known before any row - a header; [] for a headerless source
     */
    public function names(): array;

    /**
     * @param null|int<0, max> $rowBudget rows to observe from this unit, 0 for none, null for every row
     */
    public function sniff(int $unit, ?int $rowBudget): Sample;

    /**
     * Live one-unit sniffers that together cover $unit (unit 0 of each; the parent-resolved header and candidate
     * set): a source that can be cut hands out one per byte window of about $targetBytes, every other one hands out
     * [itself over $unit]. A part runs in a worker when it is Describable with a non-null describe(); the parts'
     * samples merged in order are the unit's sample.
     *
     * @param int<1, max> $targetBytes
     *
     * @return non-empty-list<self>
     */
    public function parts(int $unit, int $targetBytes): array;

    /**
     * @return int<0, max>
     */
    public function units(): int;
}
