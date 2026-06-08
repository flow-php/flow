<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\DSL;

use Flow\Telemetry\Filter\All;
use Flow\Telemetry\Filter\Any;
use Flow\Telemetry\Filter\AttributeFilter;
use Flow\Telemetry\Filter\AttributeRule;
use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Filter\Not;
use Flow\Telemetry\Logger\Middleware\AttributeFilteringLogMiddleware;
use Flow\Telemetry\Logger\Middleware\EnrichingLogMiddleware;
use Flow\Telemetry\Logger\Middleware\SeverityFilteringLogMiddleware;
use Flow\Telemetry\Logger\Processor\PipelineLogProcessor;
use Flow\Telemetry\Meter\Processor\AttributeFilteringMetricProcessor;
use Flow\Telemetry\Provider\Void\VoidLogProcessor;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Provider\Void\VoidSpanProcessor;
use Flow\Telemetry\Tests\Mother\TempDir;
use Flow\Telemetry\Tracer\Processor\AttributeFilteringSpanProcessor;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\all;
use function Flow\Telemetry\DSL\any;
use function Flow\Telemetry\DSL\attribute_filter;
use function Flow\Telemetry\DSL\attribute_filtering_log_middleware;
use function Flow\Telemetry\DSL\attribute_filtering_metric_processor;
use function Flow\Telemetry\DSL\attribute_filtering_span_processor;
use function Flow\Telemetry\DSL\attribute_rule;
use function Flow\Telemetry\DSL\enriching_log_middleware;
use function Flow\Telemetry\DSL\not;
use function Flow\Telemetry\DSL\pipeline_log_processor;
use function Flow\Telemetry\DSL\severity_filtering_log_middleware;

final class FilterFunctionsTest extends TestCase
{
    public function test_all_returns_all_matcher(): void
    {
        static::assertInstanceOf(All::class, all(attribute_rule('x', MatchMode::EQUAL, 'y')));
    }

    public function test_any_returns_any_matcher(): void
    {
        static::assertInstanceOf(Any::class, any(attribute_rule('x', MatchMode::EQUAL, 'y')));
    }

    public function test_not_returns_not_matcher(): void
    {
        static::assertInstanceOf(Not::class, not(attribute_rule('x', MatchMode::EQUAL, 'y')));
    }

    public function test_attribute_rule_returns_rule(): void
    {
        static::assertInstanceOf(AttributeRule::class, attribute_rule('x', MatchMode::EQUAL, 'y'));
    }

    public function test_attribute_filter_defaults_to_signal_source(): void
    {
        $tmp = TempDir::create();

        try {
            static::assertSame(
                [AttributeSource::SIGNAL],
                attribute_filter(attribute_rule('x', MatchMode::EQUAL, 'y'), cacheDir: $tmp->path())->sources(),
            );
        } finally {
            $tmp->remove();
        }
    }

    public function test_attribute_filter_returns_filter(): void
    {
        $tmp = TempDir::create();

        try {
            static::assertInstanceOf(AttributeFilter::class, attribute_filter(
                attribute_rule('x', MatchMode::EQUAL, 'y'),
                cacheDir: $tmp->path(),
            ));
        } finally {
            $tmp->remove();
        }
    }

    public function test_pipeline_log_processor_returns_processor(): void
    {
        static::assertInstanceOf(PipelineLogProcessor::class, pipeline_log_processor([], new VoidLogProcessor()));
    }

    public function test_enriching_log_middleware_returns_middleware(): void
    {
        static::assertInstanceOf(EnrichingLogMiddleware::class, enriching_log_middleware(['k' => 'v']));
    }

    public function test_severity_filtering_log_middleware_returns_middleware(): void
    {
        static::assertInstanceOf(SeverityFilteringLogMiddleware::class, severity_filtering_log_middleware());
    }

    public function test_attribute_filtering_log_middleware_returns_middleware(): void
    {
        $tmp = TempDir::create();

        try {
            static::assertInstanceOf(
                AttributeFilteringLogMiddleware::class,
                attribute_filtering_log_middleware(attribute_filter(
                    attribute_rule('x', MatchMode::EQUAL, 'y'),
                    cacheDir: $tmp->path(),
                )),
            );
        } finally {
            $tmp->remove();
        }
    }

    public function test_attribute_filtering_metric_processor_returns_processor(): void
    {
        $tmp = TempDir::create();

        try {
            static::assertInstanceOf(AttributeFilteringMetricProcessor::class, attribute_filtering_metric_processor(
                new VoidMetricProcessor(),
                attribute_filter(attribute_rule('x', MatchMode::EQUAL, 'y'), cacheDir: $tmp->path()),
            ));
        } finally {
            $tmp->remove();
        }
    }

    public function test_attribute_filtering_span_processor_returns_processor(): void
    {
        $tmp = TempDir::create();

        try {
            static::assertInstanceOf(AttributeFilteringSpanProcessor::class, attribute_filtering_span_processor(
                new VoidSpanProcessor(),
                attribute_filter(attribute_rule('x', MatchMode::EQUAL, 'y'), cacheDir: $tmp->path()),
            ));
        } finally {
            $tmp->remove();
        }
    }
}
