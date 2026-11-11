<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation;

interface Progress
{
    public const int ROWS_PER_CALL = 1000;

    public function advanced(int $rows): void;

    /**
     * Checks for a due beat and counts nothing.
     */
    public function busy(): void;

    /**
     * Counts in $unit from now on; the count starts again at 0. A call with the current unit does nothing.
     */
    public function countsIn(ProgressUnit $unit): void;

    /**
     * Ends this object's part of its stage; a later call opens it again.
     */
    public function done(): void;

    /**
     * Folds the totals like reported(), and never calls a heartbeat.
     *
     * @param list<StageProgress> $stages
     */
    public function ended(int $rowsRead, array $stages, int $memoryBytes, int $peakMemoryBytes): void;

    /**
     * The receiving stage stamps start and end with its own clock, because the reporter's clock is another process's.
     *
     * @param list<StageProgress> $stages
     * @param bool $progressed the reporter made progress since its last report, even when no count changed
     */
    public function reported(
        int $rowsRead,
        array $stages,
        int $memoryBytes,
        int $peakMemoryBytes,
        bool $progressed,
    ): void;

    /**
     * null: nobody listens, so a reporter sends nothing.
     */
    public function reportsEverySeconds(): ?float;
}
