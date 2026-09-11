<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

use DateTimeImmutable;
use Doctrine\DBAL\Connection\StaticServerVersionProvider;
use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Middleware\AbstractDriverMiddleware;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Platforms\DB2Platform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Override;
use SensitiveParameter;
use Throwable;

/**
 * @import-type Params from DriverManager
 */
final class TracingDriver extends AbstractDriverMiddleware
{
    /**
     * @param list<string> $excludeTables queries referencing any of these tables are not traced
     */
    public function __construct(
        private readonly Telemetry $telemetry,
        DriverInterface $driver,
        private readonly string $connectionName,
        private readonly int $maxSqlLength,
        private readonly array $excludeTables = [],
        private readonly TransactionSpanMode $transactionSpanMode = TransactionSpanMode::GROUPED,
        private readonly bool $collectMetrics = true,
        private readonly bool $includeParameters = false,
        private readonly int $maxParameters = 10,
        private readonly int $maxParameterLength = 100,
    ) {
        parent::__construct($driver);
    }

    /**
     * @param Params $params
     */
    #[Override]
    public function connect(#[SensitiveParameter] array $params): Connection
    {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $namespace = $params['dbname'] ?? 'default';

        $span = $tracer->span('doctrine.dbal.connection', SpanKind::CLIENT, [
            SemConvAttributes::DB_NAMESPACE => $namespace,
            DbAttributes::DB_CONNECTION_NAME => $this->connectionName,
        ]);
        $scope = $tracer->activate($span);

        try {
            $connection = parent::connect($params);

            $dbSystem = $this->getSemanticDbSystem($connection->getServerVersion());
            $span->setAttribute(SemConvAttributes::DB_SYSTEM_NAME, $dbSystem);

            $baseAttributes = [
                SemConvAttributes::DB_SYSTEM_NAME => $dbSystem,
                SemConvAttributes::DB_NAMESPACE => $namespace,
            ];

            $host = $params['host'] ?? null;

            if ($host !== null) {
                $baseAttributes[SemConvAttributes::SERVER_ADDRESS] = $host;
            }

            $port = $params['port'] ?? null;

            if ($port !== null) {
                $baseAttributes[SemConvAttributes::SERVER_PORT] = $port;
            }

            return new TracingConnection(
                $connection,
                new QueryTracer(
                    $this->telemetry,
                    $baseAttributes,
                    $this->maxSqlLength,
                    $this->collectMetrics,
                    $this->includeParameters,
                    $this->maxParameters,
                    $this->maxParameterLength,
                ),
                $this->transactionSpanMode,
                $baseAttributes + [DbAttributes::DB_CONNECTION_NAME => $this->connectionName],
                $this->excludeTables,
            );
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
            $tracer->complete($span);
        }
    }

    private function getSemanticDbSystem(string $serverVersion): string
    {
        $platform = $this->getDatabasePlatform(new StaticServerVersionProvider($serverVersion));

        if ($platform instanceof AbstractMySQLPlatform) {
            return 'mysql';
        }

        if ($platform instanceof PostgreSQLPlatform) {
            return 'postgresql';
        }

        if ($platform instanceof SQLitePlatform) {
            return 'sqlite';
        }

        if ($platform instanceof SQLServerPlatform) {
            return 'mssql';
        }

        if ($platform instanceof OraclePlatform) {
            return 'oracle';
        }

        if ($platform instanceof DB2Platform) {
            return 'db2';
        }

        return 'other_sql';
    }
}
