<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\DataFrame;
use Flow\ETL\Runtime\Analyze;
use Flow\ETL\Runtime\Metrics\Report;

final readonly class Run
{
    public function __construct(
        private bool|Analyze $analyze = false,
    ) {}

    public function __invoke(DataFrame $dataFrame): ?Report
    {
        return $dataFrame->run($this->analyze);
    }
}
