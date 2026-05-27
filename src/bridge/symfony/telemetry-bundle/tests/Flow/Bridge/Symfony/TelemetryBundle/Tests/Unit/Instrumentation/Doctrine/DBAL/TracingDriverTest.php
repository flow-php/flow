<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Doctrine\DBAL;

use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\API\ExceptionConverter;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\DB2Platform;
use Doctrine\DBAL\Platforms\MariaDBPlatform;
use Doctrine\DBAL\Platforms\MySQL80Platform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Doctrine\DBAL\ServerVersionProvider;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\V4\TracingDriver;
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
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;

use function interface_exists;

#[CoversClass(TracingDriver::class)]
final class TracingDriverTest extends TestCase
{
    protected function setUp(): void
    {
        if (interface_exists('Doctrine\DBAL\VersionAwarePlatformDriver')) {
            self::markTestSkipped('Test requires Doctrine DBAL 4.x');
        }
    }

    public function test_get_semantic_db_system_defaults_to_other_sql(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $platform = $this->createMock(AbstractPlatform::class);
        $driver = $this->createMockDriverWithPlatform($platform);

        $tracingDriver = new TracingDriver($telemetry, $driver, 'default', logSql: true, maxSqlLength: 100);

        $tracingDriver->connect([]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('other_sql', $spans[0]->attributes()['db.system.name']);
    }

    public function test_get_semantic_db_system_detects_db2(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $platform = new DB2Platform();
        $driver = $this->createMockDriverWithPlatform($platform);

        $tracingDriver = new TracingDriver($telemetry, $driver, 'default', logSql: true, maxSqlLength: 100);

        $tracingDriver->connect([]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('db2', $spans[0]->attributes()['db.system.name']);
    }

    public function test_get_semantic_db_system_detects_mariadb_as_mysql(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $platform = new MariaDBPlatform();
        $driver = $this->createMockDriverWithPlatform($platform);

        $tracingDriver = new TracingDriver($telemetry, $driver, 'default', logSql: true, maxSqlLength: 100);

        $tracingDriver->connect([]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('mysql', $spans[0]->attributes()['db.system.name']);
    }

    public function test_get_semantic_db_system_detects_mssql(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $platform = new SQLServerPlatform();
        $driver = $this->createMockDriverWithPlatform($platform);

        $tracingDriver = new TracingDriver($telemetry, $driver, 'default', logSql: true, maxSqlLength: 100);

        $tracingDriver->connect([]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('mssql', $spans[0]->attributes()['db.system.name']);
    }

    public function test_get_semantic_db_system_detects_mysql(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        // @mago-expect analysis:deprecated-class
        $platform = new MySQL80Platform();
        $driver = $this->createMockDriverWithPlatform($platform);

        $tracingDriver = new TracingDriver($telemetry, $driver, 'default', logSql: true, maxSqlLength: 100);

        $tracingDriver->connect([]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('mysql', $spans[0]->attributes()['db.system.name']);
    }

    public function test_get_semantic_db_system_detects_oracle(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $platform = new OraclePlatform();
        $driver = $this->createMockDriverWithPlatform($platform);

        $tracingDriver = new TracingDriver($telemetry, $driver, 'default', logSql: true, maxSqlLength: 100);

        $tracingDriver->connect([]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('oracle', $spans[0]->attributes()['db.system.name']);
    }

    public function test_get_semantic_db_system_detects_postgresql(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $platform = new PostgreSQLPlatform();
        $driver = $this->createMockDriverWithPlatform($platform);

        $tracingDriver = new TracingDriver($telemetry, $driver, 'default', logSql: true, maxSqlLength: 100);

        $tracingDriver->connect([]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('postgresql', $spans[0]->attributes()['db.system.name']);
    }

    public function test_get_semantic_db_system_detects_sqlite(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $platform = new SQLitePlatform();
        $driver = $this->createMockDriverWithPlatform($platform);

        $tracingDriver = new TracingDriver($telemetry, $driver, 'default', logSql: true, maxSqlLength: 100);

        $tracingDriver->connect([]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('sqlite', $spans[0]->attributes()['db.system.name']);
    }

    public function test_span_defaults_db_namespace_to_default(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $platform = new PostgreSQLPlatform();
        $driver = $this->createMockDriverWithPlatform($platform);

        $tracingDriver = new TracingDriver($telemetry, $driver, 'default', logSql: true, maxSqlLength: 100);

        $tracingDriver->connect([]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('default', $spans[0]->attributes()['db.namespace']);
    }

    public function test_span_includes_connection_name(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $platform = new PostgreSQLPlatform();
        $driver = $this->createMockDriverWithPlatform($platform);

        $tracingDriver = new TracingDriver($telemetry, $driver, 'analytics', logSql: true, maxSqlLength: 100);

        $tracingDriver->connect([]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('analytics', $spans[0]->attributes()['db.connection.name']);
    }

    public function test_span_includes_db_namespace_from_params(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $platform = new PostgreSQLPlatform();
        $driver = $this->createMockDriverWithPlatform($platform);

        $tracingDriver = new TracingDriver($telemetry, $driver, 'default', logSql: true, maxSqlLength: 100);

        $tracingDriver->connect(['dbname' => 'my_database']);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('my_database', $spans[0]->attributes()['db.namespace']);
    }

    private function createMockDriverWithPlatform(AbstractPlatform $platform): Driver
    {
        return new readonly class($platform) implements Driver {
            public function __construct(
                private AbstractPlatform $platform,
            ) {}

            public function connect(array $params): Connection
            {
                return new class implements Connection {
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
                        return '1.0.0';
                    }

                    public function lastInsertId(): int
                    {
                        return 0;
                    }

                    public function prepare(string $sql): Statement
                    {
                        throw new RuntimeException('Not implemented');
                    }

                    public function query(string $sql): Result
                    {
                        throw new RuntimeException('Not implemented');
                    }

                    public function quote(string $value): string
                    {
                        return "'{$value}'";
                    }

                    public function rollBack(): void {}
                };
            }

            public function getDatabasePlatform(ServerVersionProvider $versionProvider): AbstractPlatform
            {
                return $this->platform;
            }

            public function getExceptionConverter(): ExceptionConverter
            {
                throw new RuntimeException('Not implemented');
            }
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
