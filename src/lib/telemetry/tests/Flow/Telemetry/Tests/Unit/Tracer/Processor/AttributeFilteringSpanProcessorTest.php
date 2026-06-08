<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer\Processor;

use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Flow\Telemetry\Tests\Mother\SpanProcessorSpy;
use Flow\Telemetry\Tests\Mother\TempDir;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_filtering_span_processor;
use function Flow\Telemetry\DSL\attribute_rule;

final class AttributeFilteringSpanProcessorTest extends TestCase
{
    public function test_always_delegates_on_start(): void
    {
        $tmp = TempDir::create();

        try {
            $spy = new SpanProcessorSpy();
            $processor = attribute_filtering_span_processor($spy, attribute_filter(
                attribute_rule('http.route', MatchMode::EQUAL, '/health'),
                cacheDir: $tmp->path(),
            ));

            $processor->onStart(SpanMother::create('any')->setAttributes(['http.route' => '/health']));

            static::assertSame(1, $spy->startedCount());
        } finally {
            $tmp->remove();
        }
    }

    public function test_delegates_flush_and_shutdown(): void
    {
        $tmp = TempDir::create();

        try {
            $spy = new SpanProcessorSpy();
            $processor = attribute_filtering_span_processor($spy, attribute_filter(
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

    public function test_drops_span_matching_signal_attribute_on_end(): void
    {
        $tmp = TempDir::create();

        try {
            $spy = new SpanProcessorSpy();
            $processor = attribute_filtering_span_processor($spy, attribute_filter(
                attribute_rule('http.route', MatchMode::EQUAL, '/health'),
                cacheDir: $tmp->path(),
            ));

            $processor->onEnd(SpanMother::create('kept')->setAttributes(['http.route' => '/api']));
            $processor->onEnd(SpanMother::create('dropped')->setAttributes(['http.route' => '/health']));

            static::assertSame(1, $spy->endedCount());
            static::assertSame('kept', $spy->ended()[0]->name());
        } finally {
            $tmp->remove();
        }
    }

    public function test_filters_on_resource_attributes(): void
    {
        $tmp = TempDir::create();

        try {
            $spy = new SpanProcessorSpy();
            $processor = attribute_filtering_span_processor($spy, attribute_filter(
                attribute_rule('service.name', MatchMode::EQUAL, 'test-service'),
                sources: [AttributeSource::RESOURCE],
                cacheDir: $tmp->path(),
            ));

            $processor->onEnd(SpanMother::create('dropped'));

            static::assertSame(0, $spy->endedCount());
        } finally {
            $tmp->remove();
        }
    }

    public function test_filters_on_scope_attributes_keeps_span_without_matching_scope(): void
    {
        $tmp = TempDir::create();

        try {
            $spy = new SpanProcessorSpy();
            $processor = attribute_filtering_span_processor($spy, attribute_filter(
                attribute_rule('scope.flag', MatchMode::EQUAL, 'on'),
                sources: [AttributeSource::SCOPE],
                cacheDir: $tmp->path(),
            ));

            $processor->onEnd(SpanMother::create('kept'));

            static::assertSame(1, $spy->endedCount());
        } finally {
            $tmp->remove();
        }
    }
}
