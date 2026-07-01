<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

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
use function preg_match;
use function preg_quote;

final class TracingConnection extends AbstractConnectionMiddleware
{
    /**
     * @param list<string> $excludeTables queries referencing any of these tables are not traced, so
     *                                     high-churn internals (e.g. the cache_items table behind a
     *                                     Doctrine DBAL cache pool) do not surface as orphan spans
     */
    public function __construct(
        ConnectionInterface $connection,
        private readonly Telemetry $telemetry,
        private readonly bool $logSql,
        private readonly int $maxSqlLength,
        private readonly array $excludeTables = [],
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
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute('error.type', $exception::class);
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
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute('error.type', $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[Override]
    public function exec(string $sql): int|string
    {
        if ($this->isExcluded($sql)) {
            return parent::exec($sql);
        }

        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $attributes = [];

        if ($this->logSql) {
            $attributes['db.query.text'] = $this->truncateSql($sql);
        }

        $span = $tracer->span('doctrine.dbal.connection.exec', SpanKind::CLIENT, $attributes);

        try {
            return parent::exec($sql);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute('error.type', $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[Override]
    public function prepare(string $sql): DriverStatement
    {
        if ($this->isExcluded($sql)) {
            return parent::prepare($sql);
        }

        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $attributes = [];

        if ($this->logSql) {
            $attributes['db.query.text'] = $this->truncateSql($sql);
        }

        $span = $tracer->span('doctrine.dbal.statement.prepare', SpanKind::CLIENT, $attributes);

        try {
            $statement = parent::prepare($sql);

            return new TracingStatement($statement, $this->telemetry);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute('error.type', $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[Override]
    public function query(string $sql): Result
    {
        if ($this->isExcluded($sql)) {
            return parent::query($sql);
        }

        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $attributes = [];

        if ($this->logSql) {
            $attributes['db.query.text'] = $this->truncateSql($sql);
        }

        $span = $tracer->span('doctrine.dbal.connection.query', SpanKind::CLIENT, $attributes);

        try {
            return parent::query($sql);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute('error.type', $exception::class);
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
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute('error.type', $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    private function isExcluded(string $sql): bool
    {
        foreach ($this->excludeTables as $table) {
            if (preg_match('/\b' . preg_quote($table, '/') . '\b/i', $sql) === 1) {
                return true;
            }
        }

        return false;
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
