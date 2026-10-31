<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
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
     * Every batch of $batches in the backend's storage, a corrupted record as it is, a stop forwarded to $batches.
     *
     * @template T of CorruptedRecordException|Rows
     *
     * @param Generator<T> $batches what an extractor yields
     *
     * @return Generator<int, T, null|Signal, void>
     */
    public function of(Generator $batches): Generator
    {
        foreach ($batches as $batch) {
            /** @var T $adopted Rows adopted are Rows: T is kept */
            $adopted = $batch instanceof Rows ? $batch->adoptedBy($this->backend) : $batch;
            $signal = yield $adopted;

            if ($signal === Signal::STOP) {
                $batches->send(Signal::STOP);

                return;
            }
        }
    }
}
