<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Telemetry;

interface SpanAttributes
{
    /**
     * @return array<string, bool|float|int|string>
     */
    public function spanAttributes(): array;
}
