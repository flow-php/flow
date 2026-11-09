<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Flow\ETL\Foundation\Progress;

interface ReportsInference
{
    public function reportInferenceTo(Progress $progress): void;
}
