<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Telemetry\Doctrine\DBAL\V3;

use Doctrine\DBAL\Driver\Middleware\AbstractStatementMiddleware;
use Doctrine\DBAL\Driver\{Result, Statement as StatementInterface};
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\{SpanKind, SpanStatus};

final class TracingStatement extends AbstractStatementMiddleware
{
    public function __construct(
        StatementInterface $statement,
        private readonly Telemetry $telemetry,
    ) {
        parent::__construct($statement);
    }

    /**
     * @param null|array<mixed> $params
     */
    #[\Override]
    public function execute($params = null) : Result
    {
        $tracer = $this->telemetry->tracer('flow.symfony.dbal');

        $span = $tracer->span('doctrine.dbal.statement.execute', SpanKind::CLIENT);

        try {
            $result = parent::execute($params);

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
}
