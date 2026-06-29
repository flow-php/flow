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
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Override;
use SensitiveParameter;
use Throwable;

/**
 * @phpstan-import-type Params from DriverManager
 */
final class TracingDriver extends AbstractDriverMiddleware
{
    public function __construct(
        private readonly Telemetry $telemetry,
        DriverInterface $driver,
        private readonly string $connectionName,
        private readonly bool $logSql,
        private readonly int $maxSqlLength,
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

        $span = $tracer->span('doctrine.dbal.connection', SpanKind::CLIENT, [
            'db.namespace' => $params['dbname'] ?? 'default',
            'db.connection.name' => $this->connectionName,
        ]);

        try {
            $connection = parent::connect($params);

            $span->setAttribute('db.system.name', $this->getSemanticDbSystem($connection->getServerVersion()));

            return new TracingConnection($connection, $this->telemetry, $this->logSql, $this->maxSqlLength);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute('error.type', $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
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
