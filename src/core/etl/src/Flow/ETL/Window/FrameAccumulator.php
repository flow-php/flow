<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Row;

interface FrameAccumulator
{
    public function accumulate(Row $row): void;

    /**
     * Must be idempotent and non-destructive - WindowProcessor reads it after every growth step of an
     * expanding frame.
     */
    public function value(): float|int|null;
}
