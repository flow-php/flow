<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Runtime\Metrics\Processes;
use Flow\ETL\Runtime\Metrics\SourceStatistics;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Rows;
use Flow\Telemetry\ObjectExtractor;
use Generator;
use SplObjectStorage;

use function array_map;
use function array_values;
use function max;

final class RunCounts
{
    /**
     * @var SplObjectStorage<Extractor, SourceCount>
     */
    private SplObjectStorage $reads;

    /**
     * What each source declared when its read opened: the run drops a source's listing when it ends, before the
     * report is built.
     *
     * @var SplObjectStorage<Extractor, Statistics>
     */
    private SplObjectStorage $declared;

    /**
     * @var array<int, int> the peak bytes of each worker, by pid, in the order worker() first saw them
     */
    private array $workers = [];

    public function __construct()
    {
        /** @var SplObjectStorage<Extractor, SourceCount> $reads */
        $reads = new SplObjectStorage();
        $this->reads = $reads;
        /** @var SplObjectStorage<Extractor, Statistics> $declared */
        $declared = new SplObjectStorage();
        $this->declared = $declared;
    }

    /**
     * Passes $batches through unchanged. A stop is forwarded and ends the count: the consumer that sent it reads no
     * further batch, so that read never closes.
     *
     * @template T of CorruptedRecordException|Rows
     *
     * @param Generator<T> $batches what $extractor yields
     * @param bool $narrowed a limit or a partition filter was pushed into this read
     *
     * @return Generator<int, T, null|Signal, void>
     */
    public function count(Extractor $extractor, Generator $batches, bool $narrowed): Generator
    {
        yield from $this->open($extractor, $narrowed)->count($batches);
    }

    /**
     * The opened read of $extractor, created on first use: whoever drives the source counts into it and closes it.
     *
     * @param bool $narrowed a limit or a partition filter was pushed into this read
     */
    public function open(Extractor $extractor, bool $narrowed): SourceCount
    {
        if (!$this->reads->offsetExists($extractor)) {
            $this->reads[$extractor] = new SourceCount();
            $this->declared[$extractor] = $extractor->statistics();
        }

        $read = $this->reads[$extractor];
        $read->opened($narrowed);

        return $read;
    }

    /**
     * Takes over every read $other counted, as if this one had counted it; a source new to this one keeps what it
     * declared when $other opened it.
     */
    public function absorb(self $other): void
    {
        foreach ($other->reads as $extractor) {
            if (!$this->reads->offsetExists($extractor)) {
                $this->reads[$extractor] = new SourceCount();
                $this->declared[$extractor] = $other->declared[$extractor];
            }

            $this->reads[$extractor]->absorb($other->reads[$extractor]);
        }

        $this->absorbWorkers($other);
    }

    /**
     * Takes over the worker peaks $other saw and none of its reads: an abandoned attempt's workers used that memory,
     * while its source is read again.
     */
    public function absorbWorkers(self $other): void
    {
        foreach ($other->workers as $pid => $peakBytes) {
            $this->worker($pid, $peakBytes);
        }
    }

    public function worker(int $pid, int $peakBytes): void
    {
        $this->workers[$pid] = max($this->workers[$pid] ?? 0, $peakBytes);
    }

    public function processes(Unit $parent): Processes
    {
        return new Processes($parent, array_map(Unit::fromBytes(...), array_values($this->workers)));
    }

    /**
     * @return list<SourceStatistics> in the order the sources were first read
     */
    public function statistics(): array
    {
        $statistics = [];

        foreach ($this->reads as $extractor) {
            $statistics[] = new SourceStatistics(
                ObjectExtractor::shortName($extractor),
                $this->declared[$extractor],
                $this->reads[$extractor]->rows(),
                $this->reads[$extractor]->isComplete(),
            );
        }

        return $statistics;
    }
}
