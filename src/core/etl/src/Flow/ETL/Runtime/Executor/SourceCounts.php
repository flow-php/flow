<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Runtime\Metrics\SourceStatistics;
use Flow\ETL\Values\Rows;
use Flow\Telemetry\ObjectExtractor;
use Generator;
use SplObjectStorage;

final class SourceCounts
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
