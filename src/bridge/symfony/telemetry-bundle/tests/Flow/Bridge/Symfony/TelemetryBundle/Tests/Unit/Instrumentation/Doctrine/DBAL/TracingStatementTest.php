<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Doctrine\DBAL;

use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\QueryTracer;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\TracingStatement;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;

#[CoversClass(TracingStatement::class)]
final class TracingStatementTest extends TestCase
{
    public function test_traces_statement_execution(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $statement = $this->createStub(StatementInterface::class);
        $statement->method('execute')->willReturn($this->createStub(Result::class));

        $this->tracingStatement($statement, $telemetry, 'SELECT * FROM users')->execute();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('SELECT users', $spans[0]->name());
        static::assertSame(SpanKind::CLIENT, $spans[0]->kind());
        static::assertSame('SELECT * FROM users', $spans[0]->attributes()['db.query.text']);
    }

    public function test_records_exception_when_execution_fails(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $statement = $this->createStub(StatementInterface::class);
        $statement->method('execute')->willThrowException(new RuntimeException('execute failed'));

        $caught = false;

        try {
            $this->tracingStatement($statement, $telemetry, 'SELECT * FROM users')->execute();
        } catch (RuntimeException) {
            $caught = true;
        }

        static::assertTrue($caught);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertTrue($spans[0]->status()?->isError());
        static::assertSame(RuntimeException::class, $spans[0]->attributes()['error.type']);
    }

    private function tracingStatement(
        StatementInterface $statement,
        Telemetry $telemetry,
        string $sql,
    ): TracingStatement {
        $queryTracer = new QueryTracer($telemetry, [], 1000, false, false, 10, 100);

        return new TracingStatement($statement, $queryTracer, $sql, $queryTracer->extract($sql));
    }
}
