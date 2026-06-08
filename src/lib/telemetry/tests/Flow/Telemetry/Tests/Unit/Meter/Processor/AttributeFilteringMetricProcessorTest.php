<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Processor;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Tests\Mother\MetricMother;
use Flow\Telemetry\Tests\Mother\MetricProcessorSpy;
use Flow\Telemetry\Tests\Mother\TempDir;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_filtering_metric_processor;
use function Flow\Telemetry\DSL\attribute_rule;

final class AttributeFilteringMetricProcessorTest extends TestCase
{
    public function test_delegates_flush_and_shutdown(): void
    {
        $tmp = TempDir::create();

        try {
            $spy = new MetricProcessorSpy();
            $processor = attribute_filtering_metric_processor($spy, attribute_filter(
                attribute_rule('x', MatchMode::EQUAL, 'y'),
                cacheDir: $tmp->path(),
            ));

            static::assertTrue($processor->flush());
            $processor->shutdown();

            static::assertSame(1, $spy->flushCount());
            static::assertSame(1, $spy->shutdownCount());
        } finally {
            $tmp->remove();
        }
    }

    public function test_drops_metric_matching_signal_attribute(): void
    {
        $tmp = TempDir::create();

        try {
            $spy = new MetricProcessorSpy();
            $processor = attribute_filtering_metric_processor($spy, attribute_filter(
                attribute_rule('endpoint', MatchMode::CONTAINS, '/internal/'),
                cacheDir: $tmp->path(),
            ));

            $processor->process(MetricMother::counterWithAttributes('requests', 1, Attributes::create([
                'endpoint' => '/public/users',
            ])));
            $processor->process(MetricMother::counterWithAttributes('requests', 1, Attributes::create([
                'endpoint' => '/internal/health',
            ])));

            static::assertSame(1, $spy->processedCount());
            static::assertSame('/public/users', $spy->processed()[0]->attributes->get('endpoint'));
        } finally {
            $tmp->remove();
        }
    }

    public function test_filters_on_resource_attributes(): void
    {
        $tmp = TempDir::create();

        try {
            $spy = new MetricProcessorSpy();
            $processor = attribute_filtering_metric_processor($spy, attribute_filter(
                attribute_rule('service.name', MatchMode::EQUAL, 'test-service'),
                sources: [AttributeSource::RESOURCE],
                cacheDir: $tmp->path(),
            ));

            $processor->process(MetricMother::counterWithAttributes('requests', 1, Attributes::empty()));

            static::assertSame(0, $spy->processedCount());
        } finally {
            $tmp->remove();
        }
    }

    public function test_filters_on_scope_attributes_keeps_metric_without_matching_scope(): void
    {
        $tmp = TempDir::create();

        try {
            $spy = new MetricProcessorSpy();
            $processor = attribute_filtering_metric_processor($spy, attribute_filter(
                attribute_rule('scope.flag', MatchMode::EQUAL, 'on'),
                sources: [AttributeSource::SCOPE],
                cacheDir: $tmp->path(),
            ));

            $processor->process(MetricMother::counterWithAttributes('requests', 1, Attributes::empty()));

            static::assertSame(1, $spy->processedCount());
        } finally {
            $tmp->remove();
        }
    }
}
