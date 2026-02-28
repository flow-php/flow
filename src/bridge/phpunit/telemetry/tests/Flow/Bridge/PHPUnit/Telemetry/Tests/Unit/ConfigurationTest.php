<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit;

use Flow\Bridge\PHPUnit\Telemetry\Configuration;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\ConfigurationMother;
use PHPUnit\Framework\TestCase;
use PHPUnit\Runner\Extension\ParameterCollection;

final class ConfigurationTest extends TestCase
{
    public function test_creates_configuration_with_custom_otel_collector_url() : void
    {
        $parameters = ParameterCollection::fromArray(['otel_collector_url' => 'http://otel-collector:4318']);

        $config = Configuration::fromParameters($parameters);

        self::assertSame('http://otel-collector:4318', $config->otelCollectorUrl);
    }

    public function test_creates_configuration_with_custom_service_name() : void
    {
        $parameters = ParameterCollection::fromArray(['service_name' => 'my-test-suite']);

        $config = Configuration::fromParameters($parameters);

        self::assertSame('my-test-suite', $config->serviceName);
    }

    public function test_creates_configuration_with_default_values() : void
    {
        $parameters = ParameterCollection::fromArray([]);

        $config = Configuration::fromParameters($parameters);

        self::assertSame('phpunit', $config->serviceName);
        self::assertSame('http://localhost:4318', $config->otelCollectorUrl);
        self::assertTrue($config->emitTraces);
        self::assertTrue($config->emitMetrics);
        self::assertTrue($config->emitTestSpans);
        self::assertTrue($config->emitTestCaseSpans);
    }

    public function test_creates_configuration_with_metrics_disabled() : void
    {
        $parameters = ParameterCollection::fromArray(['emit_metrics' => 'false']);

        $config = Configuration::fromParameters($parameters);

        self::assertFalse($config->emitMetrics);
    }

    public function test_creates_configuration_with_test_case_spans_disabled() : void
    {
        $parameters = ParameterCollection::fromArray(['emit_test_case_spans' => 'false']);

        $config = Configuration::fromParameters($parameters);

        self::assertFalse($config->emitTestCaseSpans);
    }

    public function test_creates_configuration_with_test_spans_disabled() : void
    {
        $parameters = ParameterCollection::fromArray(['emit_test_spans' => 'false']);

        $config = Configuration::fromParameters($parameters);

        self::assertFalse($config->emitTestSpans);
    }

    public function test_creates_configuration_with_traces_disabled() : void
    {
        $parameters = ParameterCollection::fromArray(['emit_traces' => 'false']);

        $config = Configuration::fromParameters($parameters);

        self::assertFalse($config->emitTraces);
    }

    public function test_mother_creates_configuration_with_custom_service_name() : void
    {
        $config = ConfigurationMother::withCustomServiceName('custom-service');

        self::assertSame('custom-service', $config->serviceName);
    }

    public function test_mother_creates_configuration_with_disabled_metrics() : void
    {
        $config = ConfigurationMother::withDisabledMetrics();

        self::assertTrue($config->emitTraces);
        self::assertFalse($config->emitMetrics);
    }

    public function test_mother_creates_configuration_with_disabled_test_case_spans() : void
    {
        $config = ConfigurationMother::withDisabledTestCaseSpans();

        self::assertTrue($config->emitTraces);
        self::assertTrue($config->emitMetrics);
        self::assertTrue($config->emitTestSpans);
        self::assertFalse($config->emitTestCaseSpans);
    }

    public function test_mother_creates_configuration_with_disabled_test_spans() : void
    {
        $config = ConfigurationMother::withDisabledTestSpans();

        self::assertTrue($config->emitTraces);
        self::assertTrue($config->emitMetrics);
        self::assertFalse($config->emitTestSpans);
    }

    public function test_mother_creates_configuration_with_disabled_traces() : void
    {
        $config = ConfigurationMother::withDisabledTraces();

        self::assertFalse($config->emitTraces);
        self::assertTrue($config->emitMetrics);
    }

    public function test_mother_creates_default_configuration() : void
    {
        $config = ConfigurationMother::default();

        self::assertSame('phpunit', $config->serviceName);
        self::assertSame('http://localhost:4318', $config->otelCollectorUrl);
        self::assertTrue($config->emitTraces);
        self::assertTrue($config->emitMetrics);
        self::assertTrue($config->emitTestSpans);
        self::assertTrue($config->emitTestCaseSpans);
    }
}
