<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Sampler;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Tracer\Span;

final readonly class AlwaysOnSampler implements Sampler
{
    public function __toString(): string
    {
        return 'AlwaysOnSampler';
    }

    public function shouldSample(Context $parentContext, Span $span): SamplingResult
    {
        return SamplingResult::recordAndSample();
    }
}
