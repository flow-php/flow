<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

use DateTimeImmutable;
use Doctrine\DBAL\Driver\Middleware\AbstractStatementMiddleware;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Override;
use Throwable;

final class TracingStatement extends AbstractStatementMiddleware
{
    public function __construct(
        StatementInterface $statement,
        private readonly Telemetry $telemetry,
    ) {
        parent::__construct($statement);
    }

    #[Override]
    public function execute(): Result
    {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        $span = $tracer->span('doctrine.dbal.statement.execute', SpanKind::CLIENT);

        try {
            return parent::execute();
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute('error.type', $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }
}
