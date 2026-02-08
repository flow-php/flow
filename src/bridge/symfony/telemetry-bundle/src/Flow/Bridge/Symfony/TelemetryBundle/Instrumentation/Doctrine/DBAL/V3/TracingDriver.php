<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\V3;

use Doctrine\DBAL\{Driver as DriverInterface, DriverManager};
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Middleware\AbstractDriverMiddleware;
use Doctrine\DBAL\Platforms\{AbstractMySQLPlatform, DB2Platform, OraclePlatform, PostgreSQLPlatform, SQLServerPlatform, SqlitePlatform};
use Flow\Telemetry\{PackageVersion, Telemetry};
use Flow\Telemetry\Tracer\{SpanKind, SpanStatus};

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
    #[\Override]
    public function connect(
        #[\SensitiveParameter]
        array $params,
    ) : Connection {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $span = $tracer->span(
            'doctrine.dbal.connection',
            SpanKind::CLIENT,
            [
                'db.system' => $this->getSemanticDbSystem(),
                'db.namespace' => $params['dbname'] ?? 'default',
                'db.connection.name' => $this->connectionName,
            ]
        );

        try {
            $connection = parent::connect($params);

            $span->setStatus(SpanStatus::ok());

            return new TracingConnection(
                $connection,
                $this->telemetry,
                $this->logSql,
                $this->maxSqlLength,
            );
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    private function getSemanticDbSystem() : string
    {
        $platform = $this->getDatabasePlatform();

        if ($platform instanceof AbstractMySQLPlatform) {
            return 'mysql';
        }

        if ($platform instanceof PostgreSQLPlatform) {
            return 'postgresql';
        }

        if ($platform instanceof SqlitePlatform) {
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
