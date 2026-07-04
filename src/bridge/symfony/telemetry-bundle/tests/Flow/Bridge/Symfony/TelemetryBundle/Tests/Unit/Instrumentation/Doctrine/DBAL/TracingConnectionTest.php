<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Doctrine\DBAL;

use Doctrine\DBAL\Driver\Connection as ConnectionInterface;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement as DriverStatement;
use Doctrine\DBAL\ParameterType;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\QueryTracer;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\TracingConnection;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\TracingStatement;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\TransactionSpanMode;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidLogProcessor;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\SuppressingSampler;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;

use function array_map;
use function mb_strlen;
use function str_repeat;

#[CoversClass(TracingConnection::class)]
final class TracingConnectionTest extends TestCase
{
    public function test_prepare_uses_truncation(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = $this->tracingConnection($connection, $telemetry, maxSqlLength: 15);

        $sql = 'INSERT INTO users (name, email) VALUES (?, ?)';
        $tracing->prepare($sql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('INSERT INTO use...', $spans[0]->attributes()['db.query.text']);
    }

    public function test_emits_no_spans_when_tracing_is_suppressed(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage(Context::root()->withSuppressedTracing());
        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage, new SuppressingSampler(new AlwaysOnSampler())),
            new MeterProvider(new VoidMetricProcessor(), $clock),
            new LoggerProvider(new VoidLogProcessor(), $clock, $contextStorage),
        );

        $tracing = $this->tracingConnection($this->createMockConnection(), $telemetry, maxSqlLength: 100);

        $tracing->beginTransaction();
        $tracing->exec('DELETE FROM users');
        $tracing->query('SELECT * FROM users');
        $tracing->prepare('INSERT INTO users (name) VALUES (?)')->execute();
        $tracing->commit();

        static::assertCount(
            0,
            $spanProcessor->endedSpans(),
            'DBAL instrumentation must emit no spans while tracing is suppressed',
        );
    }

    public function test_query_uses_truncation(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = $this->tracingConnection($connection, $telemetry, maxSqlLength: 10);

        $sql = 'SELECT * FROM users WHERE id = 1';
        $tracing->query($sql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('SELECT * F...', $spans[0]->attributes()['db.query.text']);
    }

    public function test_query_text_is_always_recorded(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = $this->tracingConnection($connection, $telemetry, maxSqlLength: 100);

        $sql = 'SELECT * FROM users';
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('SELECT * FROM users', $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_exact_boundary_case(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = $this->tracingConnection($connection, $telemetry, maxSqlLength: 10);

        $sql = '1234567890';
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame($sql, $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_handles_multibyte_characters(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = $this->tracingConnection($connection, $telemetry, maxSqlLength: 15);

        $sql = "SELECT * FROM users WHERE name = '日本語テスト'";
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $truncated = $spans[0]->attributes()['db.query.text'];
        static::assertSame('SELECT * FROM u...', $truncated);
        static::assertSame(18, mb_strlen($truncated));
    }

    public function test_truncate_sql_returns_full_sql_when_max_length_negative(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = $this->tracingConnection($connection, $telemetry, maxSqlLength: -1);

        $longSql = str_repeat('SELECT * FROM users; ', 100);
        $tracing->exec($longSql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame($longSql, $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_returns_full_sql_when_max_length_zero(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = $this->tracingConnection($connection, $telemetry, maxSqlLength: 0);

        $longSql = str_repeat('SELECT * FROM users; ', 100);
        $tracing->exec($longSql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame($longSql, $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_returns_sql_when_shorter_than_limit(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = $this->tracingConnection($connection, $telemetry, maxSqlLength: 100);

        $sql = 'SELECT * FROM users WHERE id = 1';
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame($sql, $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_truncates_and_appends_ellipsis(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = $this->tracingConnection($connection, $telemetry, maxSqlLength: 20);

        $sql = 'SELECT * FROM users WHERE id = 1 AND status = active';
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('SELECT * FROM users ...', $spans[0]->attributes()['db.query.text']);
    }

    public function test_exec_on_excluded_table_creates_no_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            maxSqlLength: 100,
            excludeTables: ['cache_items'],
        );

        $tracing->exec('DELETE FROM cache_items WHERE item_lifetime <= 1');

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_query_on_excluded_table_creates_no_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            maxSqlLength: 100,
            excludeTables: ['cache_items'],
        );

        $tracing->query('SELECT item_data FROM cache_items WHERE item_id = 1');

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_prepare_on_excluded_table_creates_no_span_and_returns_unwrapped_statement(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            maxSqlLength: 100,
            excludeTables: ['cache_items'],
        );

        $statement = $tracing->prepare('INSERT INTO cache_items (item_id, item_data) VALUES (?, ?)');
        $statement->execute();

        static::assertNotInstanceOf(TracingStatement::class, $statement);
        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_non_excluded_table_is_still_traced(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            maxSqlLength: 100,
            excludeTables: ['cache_items'],
        );

        $tracing->query('SELECT * FROM users WHERE id = 1');

        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_exclusion_matches_whole_words_only(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            maxSqlLength: 100,
            excludeTables: ['cache'],
        );

        $tracing->query('SELECT * FROM cache_items WHERE item_id = 1');

        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_records_exception_when_operation_fails(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $connection = $this->createStub(ConnectionInterface::class);
        $connection->method('beginTransaction')->willThrowException(new RuntimeException('boom'));
        $connection->method('commit')->willThrowException(new RuntimeException('boom'));
        $connection->method('exec')->willThrowException(new RuntimeException('boom'));
        $connection->method('prepare')->willThrowException(new RuntimeException('boom'));
        $connection->method('query')->willThrowException(new RuntimeException('boom'));
        $connection->method('rollBack')->willThrowException(new RuntimeException('boom'));

        $tracing = $this->tracingConnection(
            $connection,
            $telemetry,
            maxSqlLength: 100,
            transactionSpanMode: TransactionSpanMode::PER_OPERATION,
        );

        $failures = 0;

        foreach ([
            static fn() => $tracing->beginTransaction(),
            static fn() => $tracing->commit(),
            static fn() => $tracing->exec('SELECT 1'),
            static fn() => $tracing->prepare('SELECT 1'),
            static fn() => $tracing->query('SELECT 1'),
            static fn() => $tracing->rollBack(),
        ] as $operation) {
            try {
                $operation();
            } catch (RuntimeException) {
                $failures++;
            }
        }

        static::assertSame(6, $failures);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(6, $spans);

        foreach ($spans as $span) {
            static::assertTrue($span->status()?->isError());
            static::assertSame(RuntimeException::class, $span->attributes()['error.type']);
        }
    }

    public function test_grouped_mode_wraps_queries_in_a_single_transaction_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            maxSqlLength: 100,
            transactionAttributes: ['db.system.name' => 'mysql', 'db.namespace' => 'app'],
        );

        $tracing->beginTransaction();
        $tracing->query('SELECT * FROM users');
        $tracing->commit();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(2, $spans);

        $transactionSpan = $this->spanByName($spans, 'BEGIN TRANSACTION');
        $querySpan = $this->spanByName($spans, 'SELECT users');

        static::assertSame('mysql', $transactionSpan->attributes()['db.system.name']);
        static::assertSame('app', $transactionSpan->attributes()['db.namespace']);
        static::assertFalse($transactionSpan->status()?->isError() ?? false);
        static::assertSame(
            $transactionSpan->context()->spanId->toHex(),
            $querySpan->context()->parentSpanId?->toHex(),
            'query span must nest under the transaction span',
        );
    }

    public function test_grouped_mode_clean_rollback_completes_span_without_error(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            maxSqlLength: 100,
        );

        $tracing->beginTransaction();
        $tracing->rollBack();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('BEGIN TRANSACTION', $spans[0]->name());
        static::assertFalse($spans[0]->status()?->isError() ?? false);
    }

    public function test_grouped_mode_records_error_when_rollback_throws(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());

        $connection = $this->createStub(ConnectionInterface::class);
        $connection->method('rollBack')->willThrowException(new RuntimeException('rollback boom'));

        $tracing = $this->tracingConnection(
            $connection,
            TelemetryMother::withSpanProcessor($spanProcessor),
            maxSqlLength: 100,
        );

        $tracing->beginTransaction();

        try {
            $tracing->rollBack();
        } catch (RuntimeException) {
        }

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('BEGIN TRANSACTION', $spans[0]->name());
        static::assertTrue($spans[0]->status()?->isError());
        static::assertSame(RuntimeException::class, $spans[0]->attributes()['error.type']);
    }

    public function test_grouped_mode_commit_without_open_transaction_emits_no_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            maxSqlLength: 100,
        );

        $tracing->commit();
        $tracing->rollBack();

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_per_operation_mode_emits_one_span_per_transaction_call(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            maxSqlLength: 100,
            transactionSpanMode: TransactionSpanMode::PER_OPERATION,
        );

        $tracing->beginTransaction();
        $tracing->commit();

        $spans = $spanProcessor->endedSpans();
        static::assertSame(['BEGIN TRANSACTION', 'COMMIT TRANSACTION'], array_map(static fn($s) => $s->name(), $spans));
        static::assertSame('begin', $spans[0]->attributes()['db.operation.name']);
        static::assertSame('commit', $spans[1]->attributes()['db.operation.name']);
    }

    public function test_off_mode_emits_no_transaction_spans_but_still_traces_queries(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            maxSqlLength: 100,
            transactionSpanMode: TransactionSpanMode::OFF,
        );

        $tracing->beginTransaction();
        $tracing->query('SELECT * FROM users');
        $tracing->commit();

        $spans = $spanProcessor->endedSpans();
        static::assertSame(['SELECT users'], array_map(static fn($s) => $s->name(), $spans));
    }

    public function test_grouped_mode_completes_leaked_transaction_span_on_destruct(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            maxSqlLength: 100,
        );

        $tracing->beginTransaction();

        static::assertCount(0, $spanProcessor->endedSpans(), 'transaction span stays open until commit/rollBack');

        unset($tracing);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('BEGIN TRANSACTION', $spans[0]->name());
        static::assertTrue($spans[0]->status()?->isError());
    }

    public function test_query_span_has_semantic_name_and_attributes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            baseAttributes: [
                'db.system.name' => 'postgresql',
                'db.namespace' => 'app',
                'server.address' => 'db.internal',
                'server.port' => 5433,
            ],
        );

        $tracing->query('SELECT * FROM users WHERE id = 1');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $attributes = $spans[0]->attributes();
        static::assertSame('SELECT users', $spans[0]->name());
        static::assertSame('postgresql', $attributes['db.system.name']);
        static::assertSame('app', $attributes['db.namespace']);
        static::assertSame('db.internal', $attributes['server.address']);
        static::assertSame(5433, $attributes['server.port']);
        static::assertSame('SELECT', $attributes['db.operation.name']);
        static::assertSame('users', $attributes['db.collection.name']);
        static::assertSame('SELECT * FROM users WHERE id = 1', $attributes['db.query.text']);
        static::assertArrayHasKey('db.response.returned_rows', $attributes);
    }

    public function test_records_duration_and_row_metrics_with_low_cardinality_attributes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $metricProcessor = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withProcessors($spanProcessor, $metricProcessor);

        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            $telemetry,
            baseAttributes: ['db.system.name' => 'postgresql', 'db.namespace' => 'app'],
            collectMetrics: true,
        );

        $tracing->query('SELECT * FROM users');
        $this->collectMetrics($telemetry);

        $duration = $metricProcessor->metricsWithName('db.client.operation.duration');
        static::assertCount(1, $duration);
        static::assertFalse(
            $duration[0]->attributes->has('db.query.text'),
            'query text must not be a metric dimension',
        );
        static::assertSame('postgresql', $duration[0]->attributes->get('db.system.name'));
        static::assertSame('SELECT', $duration[0]->attributes->get('db.operation.name'));
        static::assertSame('users', $duration[0]->attributes->get('db.collection.name'));

        static::assertCount(1, $metricProcessor->metricsWithName('db.client.response.returned_rows'));
    }

    public function test_transaction_duration_metric_uses_operation_and_nesting_level(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $metricProcessor = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withProcessors($spanProcessor, $metricProcessor);

        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            $telemetry,
            transactionAttributes: ['db.system.name' => 'postgresql', 'db.namespace' => 'app'],
            collectMetrics: true,
        );

        $tracing->beginTransaction();
        $tracing->commit();
        $this->collectMetrics($telemetry);

        $operations = array_map(static fn($m) => $m->attributes->get(
            'db.operation.name',
        ), $metricProcessor->metricsWithName('db.client.operation.duration'));
        static::assertContains('begin', $operations);
        static::assertContains('commit', $operations);

        foreach ($metricProcessor->metricsWithName('db.client.operation.duration') as $metric) {
            static::assertSame('postgresql', $metric->attributes->get('db.system.name'));
            static::assertSame(1, $metric->attributes->get('flow.db.transaction.nesting_level'));
            static::assertFalse(
                $metric->attributes->has('server.address'),
                'server.address must not be a metric dimension',
            );
        }
    }

    public function test_include_parameters_records_bound_values_on_the_execute_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = $this->tracingConnection(
            $this->createMockConnection(),
            TelemetryMother::withSpanProcessor($spanProcessor),
            includeParameters: true,
        );

        $statement = $tracing->prepare('SELECT * FROM users WHERE id = ?');
        $statement->bindValue(1, 42, ParameterType::INTEGER);
        $statement->execute();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(2, $spans, 'prepare span then execute span');
        static::assertSame('42', $spans[1]->attributes()['db.query.parameter.1']);
    }

    /**
     * @param array<Span> $spans
     */
    private function spanByName(array $spans, string $name): Span
    {
        foreach ($spans as $span) {
            if ($span->name() === $name) {
                return $span;
            }
        }

        static::fail("No span named {$name}");
    }

    private function createMockConnection(): ConnectionInterface
    {
        return new class implements ConnectionInterface {
            public function beginTransaction(): void {}

            public function commit(): void {}

            public function exec(string $sql): int
            {
                return 0;
            }

            public function getNativeConnection(): object
            {
                return new stdClass();
            }

            public function getServerVersion(): string
            {
                return '8.0.0';
            }

            public function lastInsertId(): int
            {
                return 0;
            }

            public function prepare(string $sql): DriverStatement
            {
                return new class implements DriverStatement {
                    public function bindValue(
                        int|string $param,
                        mixed $value,
                        ParameterType $type = ParameterType::STRING,
                    ): void {}

                    public function execute(): Result
                    {
                        return new class implements Result {
                            public function columnCount(): int
                            {
                                return 0;
                            }

                            /** @return list<array<string, mixed>> */
                            public function fetchAllAssociative(): array
                            {
                                return [];
                            }

                            /** @return array<array-key, mixed> */
                            public function fetchAllKeyValue(): array
                            {
                                return [];
                            }

                            /** @return list<list<mixed>> */
                            public function fetchAllNumeric(): array
                            {
                                return [];
                            }

                            /** @return array<string, mixed>|false */
                            public function fetchAssociative(): array|false
                            {
                                return false;
                            }

                            /** @return list<mixed> */
                            public function fetchFirstColumn(): array
                            {
                                return [];
                            }

                            /** @return false|list<mixed> */
                            public function fetchNumeric(): array|false
                            {
                                return false;
                            }

                            public function fetchOne(): mixed
                            {
                                return false;
                            }

                            public function free(): void {}

                            public function rowCount(): int
                            {
                                return 0;
                            }
                        };
                    }
                };
            }

            public function query(string $sql): Result
            {
                return new class implements Result {
                    public function columnCount(): int
                    {
                        return 0;
                    }

                    /** @return list<array<string, mixed>> */
                    public function fetchAllAssociative(): array
                    {
                        return [];
                    }

                    /** @return array<array-key, mixed> */
                    public function fetchAllKeyValue(): array
                    {
                        return [];
                    }

                    /** @return list<list<mixed>> */
                    public function fetchAllNumeric(): array
                    {
                        return [];
                    }

                    /** @return array<string, mixed>|false */
                    public function fetchAssociative(): array|false
                    {
                        return false;
                    }

                    /** @return list<mixed> */
                    public function fetchFirstColumn(): array
                    {
                        return [];
                    }

                    /** @return false|list<mixed> */
                    public function fetchNumeric(): array|false
                    {
                        return false;
                    }

                    public function fetchOne(): mixed
                    {
                        return false;
                    }

                    public function free(): void {}

                    public function rowCount(): int
                    {
                        return 0;
                    }
                };
            }

            public function quote(string $value): string
            {
                return "'{$value}'";
            }

            public function rollBack(): void {}
        };
    }

    /**
     * @param array<string, int|string> $baseAttributes
     * @param array<string, int|string> $transactionAttributes
     * @param list<string> $excludeTables
     */
    private function tracingConnection(
        ConnectionInterface $connection,
        Telemetry $telemetry,
        int $maxSqlLength = 1000,
        array $excludeTables = [],
        TransactionSpanMode $transactionSpanMode = TransactionSpanMode::GROUPED,
        array $transactionAttributes = [],
        array $baseAttributes = [],
        bool $collectMetrics = false,
        bool $includeParameters = false,
    ): TracingConnection {
        return new TracingConnection(
            $connection,
            new QueryTracer($telemetry, $baseAttributes, $maxSqlLength, $collectMetrics, $includeParameters, 10, 100),
            $transactionSpanMode,
            $transactionAttributes,
            $excludeTables,
        );
    }

    private function collectMetrics(Telemetry $telemetry): void
    {
        $meter = $telemetry->meter('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        foreach ($meter->collect() as $metric) {
            $meter->processor()->process($metric);
        }
    }
}
