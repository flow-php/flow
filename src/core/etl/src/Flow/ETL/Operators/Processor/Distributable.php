<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Operators\Processor;

interface Distributable extends Processor
{
    /**
     * null: this instance cannot be split across processes (unbound, or a shape the exchange cannot rebuild).
     */
    public function distribution(): ?Distribution;
}
