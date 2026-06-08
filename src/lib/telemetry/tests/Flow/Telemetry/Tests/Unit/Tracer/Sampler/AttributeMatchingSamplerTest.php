<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer\Sampler;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Flow\Telemetry\Tests\Mother\TempDir;
use Flow\Telemetry\Tracer\Sampler\SamplingDecision;
use Flow\Telemetry\Tracer\Span;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\always_off_sampler;
use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_matching_sampler;
use function Flow\Telemetry\DSL\attribute_rule;

final class AttributeMatchingSamplerTest extends TestCase
{
    public function test_drops_span_matching_start_attribute(): void
    {
        $tmp = TempDir::create();

        try {
            $sampler = attribute_matching_sampler(attribute_filter(
                attribute_rule('http.route', MatchMode::EQUAL, '/health'),
                cacheDir: $tmp->path(),
            ));

            static::assertSame(
                SamplingDecision::DROP,
                $sampler->shouldSample($this->spanWith(['http.route' => '/health']))->decision,
            );
        } finally {
            $tmp->remove();
        }
    }

    public function test_delegates_to_default_always_on_when_not_matching(): void
    {
        $tmp = TempDir::create();

        try {
            $sampler = attribute_matching_sampler(attribute_filter(
                attribute_rule('http.route', MatchMode::EQUAL, '/health'),
                cacheDir: $tmp->path(),
            ));

            static::assertSame(
                SamplingDecision::RECORD_AND_SAMPLE,
                $sampler->shouldSample($this->spanWith(['http.route' => '/api']))->decision,
            );
        } finally {
            $tmp->remove();
        }
    }

    public function test_non_matching_span_follows_the_delegate_sampler(): void
    {
        $tmp = TempDir::create();

        try {
            // delegate drops everything, so a non-matching span is dropped by the delegate
            $sampler = attribute_matching_sampler(
                attribute_filter(attribute_rule('http.route', MatchMode::EQUAL, '/health'), cacheDir: $tmp->path()),
                always_off_sampler(),
            );

            static::assertSame(
                SamplingDecision::DROP,
                $sampler->shouldSample($this->spanWith(['http.route' => '/api']))->decision,
            );
        } finally {
            $tmp->remove();
        }
    }

    public function test_exclude_false_keeps_only_matching_spans(): void
    {
        $tmp = TempDir::create();

        try {
            // keep ONLY matching: matching span defers to delegate (sampled), others dropped
            $sampler = attribute_matching_sampler(attribute_filter(
                attribute_rule('http.route', MatchMode::EQUAL, '/checkout'),
                exclude: false,
                cacheDir: $tmp->path(),
            ));

            static::assertSame(
                SamplingDecision::RECORD_AND_SAMPLE,
                $sampler->shouldSample($this->spanWith(['http.route' => '/checkout']))->decision,
            );
            static::assertSame(
                SamplingDecision::DROP,
                $sampler->shouldSample($this->spanWith(['http.route' => '/health']))->decision,
            );
        } finally {
            $tmp->remove();
        }
    }

    public function test_matches_on_resource_attributes(): void
    {
        $tmp = TempDir::create();

        try {
            // ResourceMother::default() carries service.name = test-service
            $sampler = attribute_matching_sampler(attribute_filter(
                attribute_rule('service.name', MatchMode::EQUAL, 'test-service'),
                sources: [AttributeSource::RESOURCE],
                cacheDir: $tmp->path(),
            ));

            static::assertSame(SamplingDecision::DROP, $sampler->shouldSample($this->spanWith([]))->decision);
        } finally {
            $tmp->remove();
        }
    }

    public function test_string_representation(): void
    {
        $tmp = TempDir::create();

        try {
            $sampler = attribute_matching_sampler(attribute_filter(
                attribute_rule('x', MatchMode::EQUAL, 'y'),
                cacheDir: $tmp->path(),
            ));

            static::assertSame('AttributeMatchingSampler', (string) $sampler);
        } finally {
            $tmp->remove();
        }
    }

    /**
     * @param array<string, bool|float|int|string> $attributes
     */
    private function spanWith(array $attributes): Span
    {
        return SpanMother::create()->setAttributes(Attributes::create($attributes));
    }
}
