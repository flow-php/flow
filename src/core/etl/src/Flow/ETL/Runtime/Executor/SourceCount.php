<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Values\Rows;
use Generator;

final class SourceCount
{
    private bool $narrowed = false;

    private int $open = 0;

    private int $rows = 0;

    public function opened(bool $narrowed): void
    {
        $this->open++;
        $this->narrowed = $this->narrowed || $narrowed;
    }

    /**
     * Counts $batches into this read, forwards a STOP, closed() once exhausted.
     *
     * @param Generator<Rows> $batches
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function count(Generator $batches): Generator
    {
        foreach ($batches as $batch) {
            $this->counted($batch->count());

            $signal = yield $batch;

            if ($signal === Signal::STOP) {
                $batches->send(Signal::STOP);

                return;
            }
        }

        $this->closed();
    }

    public function counted(int $rows): void
    {
        $this->rows += $rows;
    }

    public function closed(): void
    {
        $this->open--;
    }

    /**
     * Takes over what $other counted: its rows, its reads still open and whether one was narrowed.
     */
    public function absorb(self $other): void
    {
        $this->rows += $other->rows;
        $this->open += $other->open;
        $this->narrowed = $this->narrowed || $other->narrowed;
    }

    public function rows(): int
    {
        return $this->rows;
    }

    /**
     * Every read ran to its end and none had a limit, a partition filter or a predicate pushed into it.
     */
    public function isComplete(): bool
    {
        return $this->open === 0 && !$this->narrowed;
    }
}
