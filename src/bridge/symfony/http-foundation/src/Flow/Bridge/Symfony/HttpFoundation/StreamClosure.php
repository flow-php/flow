<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation;

use Flow\ETL\Dataset\Report;

/**
 * Interface for closures that are called after streaming completes.
 * Implementations receive the Report from the DataFrame execution.
 */
interface StreamClosure
{
    public function onComplete(?Report $report): void;
}
