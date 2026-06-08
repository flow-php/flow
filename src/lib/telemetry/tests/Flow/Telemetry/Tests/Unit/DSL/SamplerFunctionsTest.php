<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\DSL;

use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Tests\Mother\TempDir;
use Flow\Telemetry\Tracer\Sampler\AlwaysOffSampler;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\AttributeMatchingSampler;
use Flow\Telemetry\Tracer\Sampler\ParentBasedSampler;
use Flow\Telemetry\Tracer\Sampler\TraceIdRatioBasedSampler;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\always_off_sampler;
use function Flow\Telemetry\DSL\always_on_sampler;
use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_matching_sampler;
use function Flow\Telemetry\DSL\attribute_rule;
use function Flow\Telemetry\DSL\parent_based_sampler;
use function Flow\Telemetry\DSL\trace_id_ratio_based_sampler;

final class SamplerFunctionsTest extends TestCase
{
    public function test_always_on_sampler_returns_sampler(): void
    {
        static::assertInstanceOf(AlwaysOnSampler::class, always_on_sampler());
    }

    public function test_always_off_sampler_returns_sampler(): void
    {
        static::assertInstanceOf(AlwaysOffSampler::class, always_off_sampler());
    }

    public function test_trace_id_ratio_based_sampler_returns_sampler(): void
    {
        static::assertInstanceOf(TraceIdRatioBasedSampler::class, trace_id_ratio_based_sampler(0.25));
    }

    public function test_parent_based_sampler_returns_sampler(): void
    {
        static::assertInstanceOf(ParentBasedSampler::class, parent_based_sampler());
    }

    public function test_parent_based_sampler_accepts_a_root_sampler(): void
    {
        static::assertInstanceOf(ParentBasedSampler::class, parent_based_sampler(trace_id_ratio_based_sampler(0.1)));
    }

    public function test_attribute_matching_sampler_returns_sampler(): void
    {
        $tmp = TempDir::create();

        try {
            static::assertInstanceOf(
                AttributeMatchingSampler::class,
                attribute_matching_sampler(attribute_filter(
                    attribute_rule('x', MatchMode::EQUAL, 'y'),
                    cacheDir: $tmp->path(),
                )),
            );
        } finally {
            $tmp->remove();
        }
    }
}
