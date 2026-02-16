<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use function Flow\PostgreSql\DSL\postgresql_telemetry_options;
use PHPUnit\Framework\TestCase;

final class PostgreSqlTelemetryOptionsTest extends TestCase
{
    public function test_default_options_have_expected_values() : void
    {
        $options = postgresql_telemetry_options();

        self::assertTrue($options->traceQueries);
        self::assertTrue($options->traceTransactions);
        self::assertTrue($options->collectMetrics);
        self::assertFalse($options->logQueries);
        self::assertSame(1000, $options->maxQueryLength);
        self::assertFalse($options->includeParameters);
        self::assertSame(10, $options->maxParameters);
        self::assertSame(100, $options->maxParameterLength);
    }

    public function test_fluent_interface_allows_chaining() : void
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

        self::assertFalse($options->traceQueries);
        self::assertFalse($options->traceTransactions);
        self::assertFalse($options->collectMetrics);
        self::assertTrue($options->logQueries);
        self::assertSame(500, $options->maxQueryLength);
        self::assertTrue($options->includeParameters);
        self::assertSame(5, $options->maxParameters);
        self::assertSame(50, $options->maxParameterLength);
    }

    public function test_options_can_be_created_with_custom_values() : void
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

        self::assertFalse($options->traceQueries);
        self::assertFalse($options->traceTransactions);
        self::assertFalse($options->collectMetrics);
        self::assertTrue($options->logQueries);
        self::assertNull($options->maxQueryLength);
        self::assertTrue($options->includeParameters);
        self::assertNull($options->maxParameters);
        self::assertNull($options->maxParameterLength);
    }

    public function test_with_collect_metrics_creates_new_instance() : void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->collectMetrics(false);

        self::assertTrue($original->collectMetrics);
        self::assertFalse($modified->collectMetrics);
    }

    public function test_with_include_parameters_creates_new_instance() : void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->includeParameters(true);

        self::assertFalse($original->includeParameters);
        self::assertTrue($modified->includeParameters);
    }

    public function test_with_log_queries_creates_new_instance() : void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->logQueries(true);

        self::assertFalse($original->logQueries);
        self::assertTrue($modified->logQueries);
    }

    public function test_with_max_parameter_length_creates_new_instance() : void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->maxParameterLength(50);

        self::assertSame(100, $original->maxParameterLength);
        self::assertSame(50, $modified->maxParameterLength);
    }

    public function test_with_max_parameter_length_null_creates_new_instance() : void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->maxParameterLength(null);

        self::assertSame(100, $original->maxParameterLength);
        self::assertNull($modified->maxParameterLength);
    }

    public function test_with_max_parameters_creates_new_instance() : void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->maxParameters(5);

        self::assertSame(10, $original->maxParameters);
        self::assertSame(5, $modified->maxParameters);
    }

    public function test_with_max_parameters_null_creates_new_instance() : void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->maxParameters(null);

        self::assertSame(10, $original->maxParameters);
        self::assertNull($modified->maxParameters);
    }

    public function test_with_max_query_length_creates_new_instance() : void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->maxQueryLength(500);

        self::assertSame(1000, $original->maxQueryLength);
        self::assertSame(500, $modified->maxQueryLength);
    }

    public function test_with_max_query_length_null_creates_new_instance() : void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->maxQueryLength(null);

        self::assertSame(1000, $original->maxQueryLength);
        self::assertNull($modified->maxQueryLength);
    }

    public function test_with_trace_queries_creates_new_instance() : void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->traceQueries(false);

        self::assertTrue($original->traceQueries);
        self::assertFalse($modified->traceQueries);
    }

    public function test_with_trace_transactions_creates_new_instance() : void
    {
        $original = postgresql_telemetry_options();
        $modified = $original->traceTransactions(false);

        self::assertTrue($original->traceTransactions);
        self::assertFalse($modified->traceTransactions);
    }
}
