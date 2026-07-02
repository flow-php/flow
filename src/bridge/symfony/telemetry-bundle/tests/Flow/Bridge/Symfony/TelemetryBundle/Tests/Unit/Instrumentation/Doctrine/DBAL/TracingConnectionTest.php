<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Doctrine\DBAL;

use Doctrine\DBAL\Driver\Connection as ConnectionInterface;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement as DriverStatement;
use Doctrine\DBAL\ParameterType;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\TracingConnection;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\TracingStatement;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidLogProcessor;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\SuppressingSampler;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;

use function mb_strlen;
use function str_repeat;

#[CoversClass(TracingConnection::class)]
final class TracingConnectionTest extends TestCase
{
    public function test_prepare_uses_truncation(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 15);

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

        $tracing = new TracingConnection($this->createMockConnection(), $telemetry, logSql: true, maxSqlLength: 100);

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
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 10);

        $sql = 'SELECT * FROM users WHERE id = 1';
        $tracing->query($sql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('SELECT * F...', $spans[0]->attributes()['db.query.text']);
    }

    public function test_sql_not_logged_when_log_sql_disabled(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: false, maxSqlLength: 100);

        $sql = 'SELECT * FROM users';
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertArrayNotHasKey('db.query.text', $spans[0]->attributes());
    }

    public function test_truncate_sql_exact_boundary_case(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 10);

        $sql = '1234567890';
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame($sql, $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_handles_multibyte_characters(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 15);

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
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: -1);

        $longSql = str_repeat('SELECT * FROM users; ', 100);
        $tracing->exec($longSql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame($longSql, $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_returns_full_sql_when_max_length_zero(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 0);

        $longSql = str_repeat('SELECT * FROM users; ', 100);
        $tracing->exec($longSql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame($longSql, $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_returns_sql_when_shorter_than_limit(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 100);

        $sql = 'SELECT * FROM users WHERE id = 1';
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame($sql, $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_truncates_and_appends_ellipsis(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 20);

        $sql = 'SELECT * FROM users WHERE id = 1 AND status = active';
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('SELECT * FROM users ...', $spans[0]->attributes()['db.query.text']);
    }

    public function test_exec_on_excluded_table_creates_no_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = new TracingConnection(
            $this->createMockConnection(),
            $this->createTelemetry($spanProcessor),
            logSql: true,
            maxSqlLength: 100,
            excludeTables: ['cache_items'],
        );

        $tracing->exec('DELETE FROM cache_items WHERE item_lifetime <= 1');

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_query_on_excluded_table_creates_no_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = new TracingConnection(
            $this->createMockConnection(),
            $this->createTelemetry($spanProcessor),
            logSql: true,
            maxSqlLength: 100,
            excludeTables: ['cache_items'],
        );

        $tracing->query('SELECT item_data FROM cache_items WHERE item_id = 1');

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_prepare_on_excluded_table_creates_no_span_and_returns_unwrapped_statement(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = new TracingConnection(
            $this->createMockConnection(),
            $this->createTelemetry($spanProcessor),
            logSql: true,
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
        $tracing = new TracingConnection(
            $this->createMockConnection(),
            $this->createTelemetry($spanProcessor),
            logSql: true,
            maxSqlLength: 100,
            excludeTables: ['cache_items'],
        );

        $tracing->query('SELECT * FROM users WHERE id = 1');

        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_exclusion_matches_whole_words_only(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracing = new TracingConnection(
            $this->createMockConnection(),
            $this->createTelemetry($spanProcessor),
            logSql: true,
            maxSqlLength: 100,
            excludeTables: ['cache'],
        );

        $tracing->query('SELECT * FROM cache_items WHERE item_id = 1');

        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_records_exception_when_operation_fails(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createStub(ConnectionInterface::class);
        $connection->method('beginTransaction')->willThrowException(new RuntimeException('boom'));
        $connection->method('commit')->willThrowException(new RuntimeException('boom'));
        $connection->method('exec')->willThrowException(new RuntimeException('boom'));
        $connection->method('prepare')->willThrowException(new RuntimeException('boom'));
        $connection->method('query')->willThrowException(new RuntimeException('boom'));
        $connection->method('rollBack')->willThrowException(new RuntimeException('boom'));

        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 100);

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

    private function createTelemetry(MemorySpanProcessor $spanProcessor): Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            Resource::create(['service.name' => 'test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider(new VoidMetricProcessor(), $clock),
            new LoggerProvider(new VoidLogProcessor(), $clock, $contextStorage),
        );
    }
}
