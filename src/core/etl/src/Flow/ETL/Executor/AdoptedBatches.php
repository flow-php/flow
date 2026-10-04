<?php

declare(strict_types=1);

namespace Flow\ETL\Executor;

use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Rows;
use Generator;

final readonly class AdoptedBatches
{
    public function __construct(
        private Backend $backend,
    ) {}

    /**
     * Every batch of $batches in the backend's storage, a stop forwarded to $batches.
     *
     * @param Generator<int, Rows, null|Signal, void> $batches what an extractor yields
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function of(Generator $batches): Generator
    {
        foreach ($batches as $batch) {
            $signal = yield $batch->adoptedBy($this->backend);

            if ($signal === Signal::STOP) {
                $batches->send(Signal::STOP);

                return;
            }
        }
    }
}
