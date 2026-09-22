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
     * @var SplObjectStorage<Extractor, int>
     */
    private SplObjectStorage $rows;

    public function __construct()
    {
        /** @var SplObjectStorage<Extractor, int> $rows */
        $rows = new SplObjectStorage();
        $this->rows = $rows;
    }

    /**
     * Passes $batches through unchanged. A stop is forwarded and ends the count: the consumer that sent it reads no
     * further batch.
     *
     * @param Generator<int, Rows, null|Signal, void> $batches what $extractor yields
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function count(Extractor $extractor, Generator $batches): Generator
    {
        if (!$this->rows->offsetExists($extractor)) {
            $this->rows[$extractor] = 0;
        }

        foreach ($batches as $rows) {
            $this->rows[$extractor] += $rows->count();

            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                $batches->send(Signal::STOP);

                return;
            }
        }
    }

    /**
     * @return list<SourceStatistics> in the order the sources were first read
     */
    public function statistics(): array
    {
        $statistics = [];

        foreach ($this->rows as $extractor) {
            $statistics[] = new SourceStatistics(
                (new ReflectionClass($extractor))->getShortName(),
                $extractor->statistics(),
                $this->rows[$extractor],
            );
        }

        return $statistics;
    }
}
