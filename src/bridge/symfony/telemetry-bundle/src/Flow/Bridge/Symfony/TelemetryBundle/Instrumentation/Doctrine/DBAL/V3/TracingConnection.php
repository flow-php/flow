<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\V3;

use Doctrine\DBAL\Driver\Connection as ConnectionInterface;
use Doctrine\DBAL\Driver\Middleware\AbstractConnectionMiddleware;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement as DriverStatement;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;

final class TracingConnection extends AbstractConnectionMiddleware
{
    public function __construct(
        ConnectionInterface $connection,
        private readonly Telemetry $telemetry,
        private readonly bool $logSql,
        private readonly int $maxSqlLength,
    ) {
        parent::__construct($connection);
    }

    #[\Override]
    public function beginTransaction(): bool
    {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $span = $tracer->span('doctrine.dbal.transaction.begin', SpanKind::CLIENT);

        try {
            $result = parent::beginTransaction();

            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[\Override]
    public function commit(): bool
    {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $span = $tracer->span('doctrine.dbal.transaction.commit', SpanKind::CLIENT);

        try {
            $result = parent::commit();

            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[\Override]
    public function exec(string $sql): int
    {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $attributes = [];

        if ($this->logSql) {
            $attributes['db.query.text'] = $this->truncateSql($sql);
        }

        $span = $tracer->span('doctrine.dbal.connection.exec', SpanKind::CLIENT, $attributes);

        try {
            $result = parent::exec($sql);

            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[\Override]
    public function prepare(string $sql): DriverStatement
    {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $attributes = [];

        if ($this->logSql) {
            $attributes['db.query.text'] = $this->truncateSql($sql);
        }

        $span = $tracer->span('doctrine.dbal.statement.prepare', SpanKind::CLIENT, $attributes);

        try {
            $statement = parent::prepare($sql);

            $span->setStatus(SpanStatus::ok());

            return new TracingStatement($statement, $this->telemetry);
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[\Override]
    public function query(string $sql): Result
    {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $attributes = [];

        if ($this->logSql) {
            $attributes['db.query.text'] = $this->truncateSql($sql);
        }

        $span = $tracer->span('doctrine.dbal.connection.query', SpanKind::CLIENT, $attributes);

        try {
            $result = parent::query($sql);

            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[\Override]
    public function rollBack(): bool
    {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $span = $tracer->span('doctrine.dbal.transaction.rollback', SpanKind::CLIENT);

        try {
            $result = parent::rollBack();

            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    private function truncateSql(string $sql): string
    {
        if ($this->maxSqlLength <= 0) {
            return $sql;
        }

        if (\mb_strlen($sql) <= $this->maxSqlLength) {
            return $sql;
        }

        return \mb_substr($sql, 0, $this->maxSqlLength) . '...';
    }
}
