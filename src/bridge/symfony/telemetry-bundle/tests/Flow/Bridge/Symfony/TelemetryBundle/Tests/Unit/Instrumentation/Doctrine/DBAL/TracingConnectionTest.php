<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Doctrine\DBAL;

use Doctrine\DBAL\Driver\{Connection as ConnectionInterface, Result, Statement as DriverStatement};
use Doctrine\DBAL\ParameterType;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\V4\TracingConnection;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\{MemorySpanExporter, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogProcessor, VoidMetricProcessor};
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(TracingConnection::class)]
final class TracingConnectionTest extends TestCase
{
    protected function setUp() : void
    {
        if (\interface_exists('Doctrine\DBAL\VersionAwarePlatformDriver')) {
            self::markTestSkipped('Test requires Doctrine DBAL 4.x');
        }
    }

    public function test_prepare_uses_truncation() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 15);

        $sql = 'INSERT INTO users (name, email) VALUES (?, ?)';
        $tracing->prepare($sql);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('INSERT INTO use...', $spans[0]->attributes()['db.query.text']);
    }

    public function test_query_uses_truncation() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 10);

        $sql = 'SELECT * FROM users WHERE id = 1';
        $tracing->query($sql);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('SELECT * F...', $spans[0]->attributes()['db.query.text']);
    }

    public function test_sql_not_logged_when_log_sql_disabled() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: false, maxSqlLength: 100);

        $sql = 'SELECT * FROM users';
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertArrayNotHasKey('db.query.text', $spans[0]->attributes());
    }

    public function test_truncate_sql_exact_boundary_case() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 10);

        $sql = '1234567890';
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame($sql, $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_handles_multibyte_characters() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 15);

        $sql = "SELECT * FROM users WHERE name = '日本語テスト'";
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $truncated = $spans[0]->attributes()['db.query.text'];
        self::assertSame('SELECT * FROM u...', $truncated);
        self::assertSame(18, \mb_strlen($truncated));
    }

    public function test_truncate_sql_returns_full_sql_when_max_length_negative() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: -1);

        $longSql = \str_repeat('SELECT * FROM users; ', 100);
        $tracing->exec($longSql);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame($longSql, $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_returns_full_sql_when_max_length_zero() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 0);

        $longSql = \str_repeat('SELECT * FROM users; ', 100);
        $tracing->exec($longSql);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame($longSql, $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_returns_sql_when_shorter_than_limit() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 100);

        $sql = 'SELECT * FROM users WHERE id = 1';
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame($sql, $spans[0]->attributes()['db.query.text']);
    }

    public function test_truncate_sql_truncates_and_appends_ellipsis() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $connection = $this->createMockConnection();
        $tracing = new TracingConnection($connection, $telemetry, logSql: true, maxSqlLength: 20);

        $sql = 'SELECT * FROM users WHERE id = 1 AND status = active';
        $tracing->exec($sql);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('SELECT * FROM users ...', $spans[0]->attributes()['db.query.text']);
    }

    private function createMockConnection() : ConnectionInterface
    {
        return new class implements ConnectionInterface {
            public function beginTransaction() : void
            {
            }

            public function commit() : void
            {
            }

            public function exec(string $sql) : int
            {
                return 0;
            }

            public function getNativeConnection() : object
            {
                return new \stdClass();
            }

            public function getServerVersion() : string
            {
                return '8.0.0';
            }

            public function lastInsertId() : int
            {
                return 0;
            }

            public function prepare(string $sql) : DriverStatement
            {
                return new class implements DriverStatement {
                    public function bindValue(int|string $param, mixed $value, ParameterType $type = ParameterType::STRING) : void
                    {
                    }

                    public function execute() : Result
                    {
                        return new class implements Result {
                            public function columnCount() : int
                            {
                                return 0;
                            }

                            /** @return list<array<string, mixed>> */
                            public function fetchAllAssociative() : array
                            {
                                return [];
                            }

                            /** @return array<mixed, mixed> */
                            public function fetchAllKeyValue() : array
                            {
                                return [];
                            }

                            /** @return list<list<mixed>> */
                            public function fetchAllNumeric() : array
                            {
                                return [];
                            }

                            /** @return array<string, mixed>|false */
                            public function fetchAssociative() : array|false
                            {
                                return false;
                            }

                            /** @return list<mixed> */
                            public function fetchFirstColumn() : array
                            {
                                return [];
                            }

                            /** @return false|list<mixed> */
                            public function fetchNumeric() : array|false
                            {
                                return false;
                            }

                            public function fetchOne() : mixed
                            {
                                return false;
                            }

                            public function free() : void
                            {
                            }

                            public function rowCount() : int
                            {
                                return 0;
                            }
                        };
                    }
                };
            }

            public function query(string $sql) : Result
            {
                return new class implements Result {
                    public function columnCount() : int
                    {
                        return 0;
                    }

                    /** @return list<array<string, mixed>> */
                    public function fetchAllAssociative() : array
                    {
                        return [];
                    }

                    /** @return array<mixed, mixed> */
                    public function fetchAllKeyValue() : array
                    {
                        return [];
                    }

                    /** @return list<list<mixed>> */
                    public function fetchAllNumeric() : array
                    {
                        return [];
                    }

                    /** @return array<string, mixed>|false */
                    public function fetchAssociative() : array|false
                    {
                        return false;
                    }

                    /** @return list<mixed> */
                    public function fetchFirstColumn() : array
                    {
                        return [];
                    }

                    /** @return false|list<mixed> */
                    public function fetchNumeric() : array|false
                    {
                        return false;
                    }

                    public function fetchOne() : mixed
                    {
                        return false;
                    }

                    public function free() : void
                    {
                    }

                    public function rowCount() : int
                    {
                        return 0;
                    }
                };
            }

            public function quote(string $value) : string
            {
                return "'{$value}'";
            }

            public function rollBack() : void
            {
            }
        };
    }

    private function createTelemetry(MemorySpanProcessor $spanProcessor) : Telemetry
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
