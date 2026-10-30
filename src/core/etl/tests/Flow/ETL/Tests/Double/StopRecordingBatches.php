<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Values\Rows;
use Generator;

use function array_values;

final class StopRecordingBatches
{
    public int $read = 0;

    public bool $stopped = false;

    /**
     * @var list<Rows>
     */
    private readonly array $batches;

    public function __construct(Rows ...$batches)
    {
        $this->batches = array_values($batches);
    }

    /**
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function rows(): Generator
    {
        foreach ($this->batches as $batch) {
            $this->read++;
            $signal = yield $batch;

            if ($signal === Signal::STOP) {
                $this->stopped = true;

                return;
            }
        }
    }
}
