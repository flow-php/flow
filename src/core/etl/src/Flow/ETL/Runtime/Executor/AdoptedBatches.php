<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
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
