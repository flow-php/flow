<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

/**
 * Configuration options for PostgreSQL client telemetry.
 *
 * Controls which telemetry signals (traces, metrics, logs) are enabled
 * and how query information is captured.
 */
final readonly class PostgreSqlTelemetryOptions
{
    /**
     * @param bool $traceQueries Whether to create spans for query execution
     * @param bool $traceTransactions Whether to create spans for transactions
     * @param bool $collectMetrics Whether to collect metrics (duration, row counts)
     * @param bool $logQueries Whether to log executed queries
     * @param null|int $maxQueryLength Maximum length of query text in telemetry (null = unlimited)
     * @param bool $includeParameters Whether to include query parameters in telemetry
     * @param null|int $maxParameters Maximum number of parameters to include in telemetry (null = unlimited)
     * @param null|int $maxParameterLength Maximum length of parameter values in telemetry (null = unlimited)
     */
    public function __construct(
        public bool $traceQueries = true,
        public bool $traceTransactions = true,
        public bool $collectMetrics = true,
        public bool $logQueries = false,
        public ?int $maxQueryLength = 1000,
        public bool $includeParameters = false,
        public ?int $maxParameters = 10,
        public ?int $maxParameterLength = 100,
    ) {
    }

    public function collectMetrics(bool $collect = true) : self
    {
        return new self(
            $this->traceQueries,
            $this->traceTransactions,
            $collect,
            $this->logQueries,
            $this->maxQueryLength,
            $this->includeParameters,
            $this->maxParameters,
            $this->maxParameterLength,
        );
    }

    public function includeParameters(bool $include = true) : self
    {
        return new self(
            $this->traceQueries,
            $this->traceTransactions,
            $this->collectMetrics,
            $this->logQueries,
            $this->maxQueryLength,
            $include,
            $this->maxParameters,
            $this->maxParameterLength,
        );
    }

    public function logQueries(bool $log = true) : self
    {
        return new self(
            $this->traceQueries,
            $this->traceTransactions,
            $this->collectMetrics,
            $log,
            $this->maxQueryLength,
            $this->includeParameters,
            $this->maxParameters,
            $this->maxParameterLength,
        );
    }

    public function maxParameterLength(?int $length) : self
    {
        return new self(
            $this->traceQueries,
            $this->traceTransactions,
            $this->collectMetrics,
            $this->logQueries,
            $this->maxQueryLength,
            $this->includeParameters,
            $this->maxParameters,
            $length,
        );
    }

    public function maxParameters(?int $max) : self
    {
        return new self(
            $this->traceQueries,
            $this->traceTransactions,
            $this->collectMetrics,
            $this->logQueries,
            $this->maxQueryLength,
            $this->includeParameters,
            $max,
            $this->maxParameterLength,
        );
    }

    public function maxQueryLength(?int $length) : self
    {
        return new self(
            $this->traceQueries,
            $this->traceTransactions,
            $this->collectMetrics,
            $this->logQueries,
            $length,
            $this->includeParameters,
            $this->maxParameters,
            $this->maxParameterLength,
        );
    }

    public function traceQueries(bool $trace = true) : self
    {
        return new self(
            $trace,
            $this->traceTransactions,
            $this->collectMetrics,
            $this->logQueries,
            $this->maxQueryLength,
            $this->includeParameters,
            $this->maxParameters,
            $this->maxParameterLength,
        );
    }

    public function traceTransactions(bool $trace = true) : self
    {
        return new self(
            $this->traceQueries,
            $trace,
            $this->collectMetrics,
            $this->logQueries,
            $this->maxQueryLength,
            $this->includeParameters,
            $this->maxParameters,
            $this->maxParameterLength,
        );
    }
}
