<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Telemetry;

use Flow\Telemetry\Context\Scope;
use Flow\Telemetry\Tracer\Span;

final readonly class SpanScope
{
    public function __construct(
        public Span $span,
        public Scope $scope,
    ) {}
}
