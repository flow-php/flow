<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Metrics;

use Flow\ETL\Values\Memory\Unit;

use function array_map;

final readonly class Processes
{
    /**
     * @param list<Unit> $workers the peak of each worker, in the order they first completed a task; none for a serial
     *                            run
     */
    public function __construct(
        public Unit $parent,
        public array $workers,
    ) {}

    /**
     * @return array{parent: int, workers: list<int>, worker_peak: null|int, peak_sum: int} in bytes
     */
    public function normalize(): array
    {
        return [
            'parent' => $this->parent->inBytes(),
            'workers' => array_map(static fn(Unit $worker): int => $worker->inBytes(), $this->workers),
            'worker_peak' => $this->workerPeak()?->inBytes(),
            'peak_sum' => $this->peakSum()->inBytes(),
        ];
    }

    /**
     * Parent plus every worker. An upper bound for sizing, not the memory the run used: the peaks happen at different
     * times and each process counts its shared libraries.
     */
    public function peakSum(): Unit
    {
        $bytes = $this->parent->inBytes();

        foreach ($this->workers as $worker) {
            $bytes += $worker->inBytes();
        }

        return Unit::fromBytes($bytes);
    }

    public function workerPeak(): ?Unit
    {
        $peak = null;

        foreach ($this->workers as $worker) {
            if ($peak === null || $worker->isGreaterThan($peak)) {
                $peak = $worker;
            }
        }

        return $peak;
    }
}
