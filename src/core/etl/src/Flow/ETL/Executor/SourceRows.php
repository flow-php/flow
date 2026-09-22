<?php

declare(strict_types=1);

namespace Flow\ETL\Executor;

use Flow\ETL\Dataset\SourceStatistics;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Rows;
use Generator;
use ReflectionClass;
use SplObjectStorage;

final class SourceRows
{
    /**
     * @var SplObjectStorage<Extractor, SourceRead>
     */
    private SplObjectStorage $reads;

    public function __construct()
    {
        /** @var SplObjectStorage<Extractor, SourceRead> $reads */
        $reads = new SplObjectStorage();
        $this->reads = $reads;
    }

    /**
     * Passes $batches through unchanged. A stop is forwarded and ends the count: the consumer that sent it reads no
     * further batch, so that read never closes.
     *
     * @param Generator<int, Rows, null|Signal, void> $batches what $extractor yields
     * @param bool $narrowed a limit or a partition filter was pushed into this read
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function count(Extractor $extractor, Generator $batches, bool $narrowed): Generator
    {
        if (!$this->reads->offsetExists($extractor)) {
            $this->reads[$extractor] = new SourceRead();
        }

        $read = $this->reads[$extractor];
        $read->opened($narrowed);

        foreach ($batches as $batch) {
            $read->counted($batch->count());

            $signal = yield $batch;

            if ($signal === Signal::STOP) {
                $batches->send(Signal::STOP);

                return;
            }
        }

        $read->closed();
    }

    /**
     * @return list<SourceStatistics> in the order the sources were first read
     */
    public function statistics(): array
    {
        $statistics = [];

        foreach ($this->reads as $extractor) {
            $statistics[] = new SourceStatistics(
                (new ReflectionClass($extractor))->getShortName(),
                $extractor->statistics(),
                $this->reads[$extractor]->rows(),
                $this->reads[$extractor]->isComplete(),
            );
        }

        return $statistics;
    }
}
