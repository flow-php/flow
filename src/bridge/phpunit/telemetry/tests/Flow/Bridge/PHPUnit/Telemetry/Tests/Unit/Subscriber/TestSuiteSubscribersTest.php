<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit\Subscriber;

use Flow\Bridge\PHPUnit\Telemetry\SpanStack;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestSuiteFinishedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\Subscriber\TestSuiteStartedSubscriber;
use Flow\Bridge\PHPUnit\Telemetry\SuiteOutcomeStack;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\ConfigurationMother;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\TelemetryMother;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\TestEventMother;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\memory_metric_processor;
use function Flow\Telemetry\DSL\memory_span_processor;
use function Flow\Telemetry\DSL\void_exporter;

final class TestSuiteSubscribersTest extends TestCase
{
    public function test_suite_run_status_is_failure_when_a_test_failed(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::default();
        $suiteOutcomes = new SuiteOutcomeStack();

        $started = new TestSuiteStartedSubscriber($telemetry, $spanStack, $config, $suiteOutcomes);
        $finished = new TestSuiteFinishedSubscriber($telemetry, $spanStack, $config, $suiteOutcomes);

        $started->notify(TestEventMother::suiteStarted('Example Suite'));
        $suiteOutcomes->recordStatus('failed');
        $finished->notify(TestEventMother::suiteFinished('Example Suite'));

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('failure', $spans[0]->attributes()['test.suite.run.status']);
    }

    public function test_suite_run_status_is_skipped_when_all_tests_skipped(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::default();
        $suiteOutcomes = new SuiteOutcomeStack();

        $started = new TestSuiteStartedSubscriber($telemetry, $spanStack, $config, $suiteOutcomes);
        $finished = new TestSuiteFinishedSubscriber($telemetry, $spanStack, $config, $suiteOutcomes);

        $started->notify(TestEventMother::suiteStarted('Example Suite'));
        $suiteOutcomes->recordStatus('skipped');
        $finished->notify(TestEventMother::suiteFinished('Example Suite'));

        static::assertSame('skipped', $spanProcessor->endedSpans()[0]->attributes()['test.suite.run.status']);
    }

    public function test_suite_span_carries_official_and_flow_attributes(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::default();
        $suiteOutcomes = new SuiteOutcomeStack();

        $started = new TestSuiteStartedSubscriber($telemetry, $spanStack, $config, $suiteOutcomes);
        $finished = new TestSuiteFinishedSubscriber($telemetry, $spanStack, $config, $suiteOutcomes);

        $started->notify(TestEventMother::suiteStarted('Example Suite', 3));
        $finished->notify(TestEventMother::suiteFinished('Example Suite', 3));

        $attributes = $spanProcessor->endedSpans()[0]->attributes();
        static::assertSame('Example Suite', $attributes['test.suite.name']);
        static::assertSame(3, $attributes['flow.phpunit.suite.test_count']);
        static::assertFalse($attributes['flow.phpunit.suite.is_root']);
        static::assertSame('success', $attributes['test.suite.run.status']);
    }

    public function test_suite_metrics_use_ucum_units_and_seconds_values(): void
    {
        $metricProcessor = memory_metric_processor(void_exporter());
        $spanProcessor = memory_span_processor(void_exporter());
        $telemetry = TelemetryMother::create($spanProcessor, $metricProcessor);
        $spanStack = new SpanStack();
        $config = ConfigurationMother::default();
        $suiteOutcomes = new SuiteOutcomeStack();

        $started = new TestSuiteStartedSubscriber($telemetry, $spanStack, $config, $suiteOutcomes);
        $finished = new TestSuiteFinishedSubscriber($telemetry, $spanStack, $config, $suiteOutcomes);

        $started->notify(TestEventMother::suiteStarted('Example Suite', 2));
        $finished->notify(TestEventMother::suiteFinished('Example Suite', 2));
        $telemetry->flush();

        $durationMetrics = $metricProcessor->metricsWithName('flow.phpunit.suite.duration');
        static::assertNotEmpty($durationMetrics);
        static::assertSame('s', $durationMetrics[0]->unit);

        $suiteSpanDurationMs = $spanProcessor->endedSpans()[0]->duration();
        static::assertNotNull($suiteSpanDurationMs);
        static::assertEqualsWithDelta($suiteSpanDurationMs / 1_000, $durationMetrics[0]->value, 0.000_001);

        $countMetrics = $metricProcessor->metricsWithName('flow.phpunit.suite.test_count');
        static::assertNotEmpty($countMetrics);
        static::assertSame('{test}', $countMetrics[0]->unit);
        static::assertSame(2, $countMetrics[0]->value);
        static::assertSame('Example Suite', $countMetrics[0]->attributes->get('test.suite.name'));
    }
}
