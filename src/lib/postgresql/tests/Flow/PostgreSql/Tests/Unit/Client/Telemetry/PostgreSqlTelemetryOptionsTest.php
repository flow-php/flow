<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\postgresql_telemetry_options;

final class PostgreSqlTelemetryOptionsTest extends TestCase
{
    public function test_default_options_have_expected_values(): void
    {
        $options = postgresql_telemetry_options();

        static::assertTrue($options->traceQueries);
        static::assertTrue($options->traceTransactions);
        static::assertTrue($options->collectMetrics);
        static::assertFalse($options->logQueries);
        static::assertSame(1000, $options->maxQueryLength);
        static::assertFalse($options->includeParameters);
        static::assertSame(10, $options->maxParameters);
        static::assertSame(100, $options->maxParameterLength);
    }

    public function test_fluent_interface_allows_chaining(): void
    {
        $options = postgresql_telemetry_options()
            ->traceQueries(false)
            ->traceTransactions(false)
            ->collectMetrics(false)
            ->logQueries(true)
            ->maxQueryLength(500)
            ->includeParameters(true)
            ->maxParameters(5)
            ->maxParameterLength(50);

        static::assertFalse($options->traceQueries);
        static::assertFalse($options->traceTransactions);
        static::assertFalse($options->collectMetrics);
        static::assertTrue($options->logQueries);
        static::assertSame(500, $options->maxQueryLength);
        static::assertTrue($options->includeParameters);
        static::assertSame(5, $options->maxParameters);
        static::assertSame(50, $options->maxParameterLength);
    }

    public function test_options_can_be_created_with_custom_values(): void
    {
        $options = postgresql_telemetry_options(
            traceQueries: false,
            traceTransactions: false,
            collectMetrics: false,
            logQueries: true,
            maxQueryLength: null,
            includeParameters: true,
            maxParameters: null,
            maxParameterLength: null,
        );

        static::assertFalse($options->traceQueries);
        static::assertFalse($options->traceTransactions);
        static::assertFalse($options->collectMetrics);
        static::assertTrue($options->logQueries);
        static::assertNull($options->maxQueryLength);
        static::assertTrue($options->includeParameters);
        static::assertNull($options->maxParameters);
        static::assertNull($options->maxParameterLength);
    }

    public function test_with_collect_metrics_creates_new_instance(): void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->collectMetrics(false);

        static::assertTrue($original->collectMetrics);
        static::assertFalse($modified->collectMetrics);
    }

    public function test_with_include_parameters_creates_new_instance(): void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->includeParameters(true);

        static::assertFalse($original->includeParameters);
        static::assertTrue($modified->includeParameters);
    }

    public function test_with_log_queries_creates_new_instance(): void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->logQueries(true);

        static::assertFalse($original->logQueries);
        static::assertTrue($modified->logQueries);
    }

    public function test_with_max_parameter_length_creates_new_instance(): void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->maxParameterLength(50);

        static::assertSame(100, $original->maxParameterLength);
        static::assertSame(50, $modified->maxParameterLength);
    }

    public function test_with_max_parameter_length_null_creates_new_instance(): void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->maxParameterLength(null);

        static::assertSame(100, $original->maxParameterLength);
        static::assertNull($modified->maxParameterLength);
    }

    public function test_with_max_parameters_creates_new_instance(): void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->maxParameters(5);

        static::assertSame(10, $original->maxParameters);
        static::assertSame(5, $modified->maxParameters);
    }

    public function test_with_max_parameters_null_creates_new_instance(): void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->maxParameters(null);

        static::assertSame(10, $original->maxParameters);
        static::assertNull($modified->maxParameters);
    }

    public function test_with_max_query_length_creates_new_instance(): void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->maxQueryLength(500);

        static::assertSame(1000, $original->maxQueryLength);
        static::assertSame(500, $modified->maxQueryLength);
    }

    public function test_with_max_query_length_null_creates_new_instance(): void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->maxQueryLength(null);

        static::assertSame(1000, $original->maxQueryLength);
        static::assertNull($modified->maxQueryLength);
    }

    public function test_with_trace_queries_creates_new_instance(): void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->traceQueries(false);

        static::assertTrue($original->traceQueries);
        static::assertFalse($modified->traceQueries);
    }

    public function test_with_trace_transactions_creates_new_instance(): void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->traceTransactions(false);

        static::assertTrue($original->traceTransactions);
        static::assertFalse($modified->traceTransactions);
    }
}
