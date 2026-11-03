<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\DataFrame;
use Flow\ETL\Planning\Logical\Plan;
use Flow\ETL\Planning\Logical\Trigger;

final readonly class Explain
{
    public function __construct(
        private Trigger $trigger = Trigger::run,
    ) {}

    public function __invoke(DataFrame $dataFrame): Plan
    {
        return $dataFrame->explain($this->trigger);
    }
}
