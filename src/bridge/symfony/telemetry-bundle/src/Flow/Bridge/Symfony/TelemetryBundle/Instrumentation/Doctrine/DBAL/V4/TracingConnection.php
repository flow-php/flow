<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\V4;

use DateTimeImmutable;
use Doctrine\DBAL\Driver\Connection as ConnectionInterface;
use Doctrine\DBAL\Driver\Middleware\AbstractConnectionMiddleware;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement as DriverStatement;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Override;
use Throwable;

use function mb_strlen;
use function mb_substr;

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

    #[Override]
    public function beginTransaction(): void
    {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $span = $tracer->span('doctrine.dbal.transaction.begin', SpanKind::CLIENT);

        try {
            parent::beginTransaction();

            $span->setStatus(SpanStatus::ok());
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[Override]
    public function commit(): void
    {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $span = $tracer->span('doctrine.dbal.transaction.commit', SpanKind::CLIENT);

        try {
            parent::commit();

            $span->setStatus(SpanStatus::ok());
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[Override]
    public function exec(string $sql): int|string
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
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[Override]
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
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[Override]
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
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[Override]
    public function rollBack(): void
    {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $span = $tracer->span('doctrine.dbal.transaction.rollback', SpanKind::CLIENT);

        try {
            parent::rollBack();

            $span->setStatus(SpanStatus::ok());
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
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

        if (mb_strlen($sql) <= $this->maxSqlLength) {
            return $sql;
        }

        return mb_substr($sql, 0, $this->maxSqlLength) . '...';
    }
}
