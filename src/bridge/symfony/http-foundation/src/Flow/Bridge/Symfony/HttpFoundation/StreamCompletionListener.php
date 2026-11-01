<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation;

use Flow\ETL\Runtime\Metrics\Report;

interface StreamCompletionListener
{
    public function onComplete(?Report $report): void;
}
