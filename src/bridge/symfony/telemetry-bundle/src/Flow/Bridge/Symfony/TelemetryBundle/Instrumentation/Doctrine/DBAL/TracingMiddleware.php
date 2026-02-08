<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\Middleware as MiddlewareInterface;
use Flow\Telemetry\Telemetry;

final readonly class TracingMiddleware implements MiddlewareInterface
{
    /**
     * @param class-string $driverClass
     */
    public function __construct(
        private Telemetry $telemetry,
        private string $driverClass,
        private string $connectionName,
        private bool $logSql = true,
        private int $maxSqlLength = 1000,
    ) {
    }

    public function wrap(DriverInterface $driver) : DriverInterface
    {
        return new ($this->driverClass)(
            $this->telemetry,
            $driver,
            $this->connectionName,
            $this->logSql,
            $this->maxSqlLength,
        );
    }
}
