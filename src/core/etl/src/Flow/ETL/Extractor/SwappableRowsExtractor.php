<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Generator;

final class SwappableRowsExtractor implements Extractor
{
    private Rows $rows;

    private bool $stopped = false;

    public function __construct()
    {
        $this->rows = new Rows();
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $signal = yield $this->rows;

        if ($signal === Signal::STOP) {
            $this->stopped = true;

            return;
        }

        $signal = yield new Rows();

        if ($signal === Signal::STOP) {
            $this->stopped = true;
        }
    }

    public function stopped(): bool
    {
        return $this->stopped;
    }

    public function swap(Rows $rows): void
    {
        $this->rows = $rows;
    }
}
