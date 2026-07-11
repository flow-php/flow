<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\Middleware as MiddlewareInterface;
use Flow\Telemetry\Telemetry;

final readonly class TracingMiddleware implements MiddlewareInterface
{
    /**
     * @param class-string<DriverInterface> $driverClass
     * @param list<string> $excludeTables queries referencing any of these tables are not traced
     */
    public function __construct(
        private Telemetry $telemetry,
        private string $driverClass,
        private string $connectionName,
        private int $maxSqlLength = 1000,
        private array $excludeTables = [],
        private TransactionSpanMode $transactionSpanMode = TransactionSpanMode::GROUPED,
        private bool $collectMetrics = true,
        private bool $includeParameters = false,
        private int $maxParameters = 10,
        private int $maxParameterLength = 100,
    ) {}

    public function wrap(DriverInterface $driver): DriverInterface
    {
        // @mago-expect analysis:unsafe-instantiation,too-many-arguments
        return new $this->driverClass(
            $this->telemetry,
            $driver,
            $this->connectionName,
            $this->maxSqlLength,
            $this->excludeTables,
            $this->transactionSpanMode,
            $this->collectMetrics,
            $this->includeParameters,
            $this->maxParameters,
            $this->maxParameterLength,
        );
    }
}
