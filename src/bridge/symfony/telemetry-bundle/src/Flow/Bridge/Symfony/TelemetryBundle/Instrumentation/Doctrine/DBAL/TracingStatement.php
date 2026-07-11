<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

use Doctrine\DBAL\Driver\Middleware\AbstractStatementMiddleware;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Doctrine\DBAL\ParameterType;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\Tracer\SpanKind;
use Override;
use Throwable;

use function hrtime;

final class TracingStatement extends AbstractStatementMiddleware
{
    /**
     * @var array<int|string, mixed>
     */
    private array $boundParameters = [];

    public function __construct(
        StatementInterface $statement,
        private readonly QueryTracer $queryTracer,
        private readonly string $sql,
        private readonly SqlAttributes $sqlAttributes,
    ) {
        parent::__construct($statement);
    }

    #[Override]
    public function bindValue(int|string $param, mixed $value, ParameterType $type): void
    {
        $this->boundParameters[$param] = $value;

        parent::bindValue($param, $value, $type);
    }

    #[Override]
    public function execute(): Result
    {
        $startTime = hrtime(true);
        $tracer = $this->queryTracer->tracer();
        $span = $tracer->span(
            $this->queryTracer->spanName($this->sqlAttributes),
            SpanKind::CLIENT,
            $this->queryTracer->queryAttributes($this->sql, $this->sqlAttributes)
            + $this->queryTracer->parameterAttributes($this->boundParameters),
        );

        try {
            $result = parent::execute();
            $rows = (int) $result->rowCount();
            $span->setAttribute(SemConvAttributes::DB_RESPONSE_RETURNED_ROWS, $rows);
            $this->queryTracer->recordQueryMetrics($startTime, $rows, $this->sqlAttributes);

            return $result;
        } catch (Throwable $exception) {
            $this->queryTracer->recordError($span, $exception);
            $this->queryTracer->recordQueryMetrics($startTime, null, $this->sqlAttributes);

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }
}
