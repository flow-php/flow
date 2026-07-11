<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Doctrine\DBAL;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Driver\Middleware as DBALMiddlewareInterface;
use Doctrine\DBAL\DriverManager;
use Exception;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\DBALTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\DependencyInjection\ContainerBuilder;

use function array_filter;
use function array_map;
use function array_values;
use function interface_exists;
use function is_string;
use function mb_strlen;

#[CoversClass(TracingMiddleware::class)]
#[CoversClass(DBALTelemetryPass::class)]
final class TracingMiddlewareTest extends KernelTestCase
{
    protected function setUp(): void
    {
        if (!interface_exists(DBALMiddlewareInterface::class)) {
            self::markTestSkipped('doctrine/dbal is not installed');
        }

        parent::setUp();
    }

    public function test_connection_span_includes_db_system_attribute(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => ['enabled' => true],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingMiddleware $middleware */
        $middleware = $container->get('flow.telemetry.dbal.middleware.default');

        $configuration = new Configuration();
        $configuration->setMiddlewares([$middleware]);

        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ], $configuration);

        $connection->executeQuery('SELECT 1');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $connectionSpan = null;

        foreach ($spans as $span) {
            if ($span->name() === 'doctrine.dbal.connection') {
                $connectionSpan = $span;
            }
        }

        static::assertNotNull($connectionSpan, 'Connection span should exist');
        static::assertSame('sqlite', $connectionSpan->attributes()['db.system.name']);
        static::assertSame('default', $connectionSpan->attributes()['flow.db.connection.name']);
    }

    public function test_dbal_middleware_is_not_registered_when_feature_disabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => ['enabled' => false],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertFalse($container->has('flow.telemetry.dbal.middleware.default'));
    }

    public function test_dbal_middleware_is_registered_for_multiple_connections(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', [
                        'default' => 'doctrine.dbal.default_connection',
                        'secondary' => 'doctrine.dbal.secondary_connection',
                    ]);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => ['enabled' => true],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.dbal.middleware.default'));
        static::assertTrue($container->has('flow.telemetry.dbal.middleware.secondary'));
        static::assertInstanceOf(TracingMiddleware::class, $container->get('flow.telemetry.dbal.middleware.default'));
        static::assertInstanceOf(TracingMiddleware::class, $container->get('flow.telemetry.dbal.middleware.secondary'));
    }

    public function test_dbal_middleware_is_registered_when_feature_enabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => ['enabled' => true],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.dbal.middleware.default'));
        static::assertInstanceOf(TracingMiddleware::class, $container->get('flow.telemetry.dbal.middleware.default'));
    }

    public function test_excluded_connection_by_exact_name_is_not_traced(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', [
                        'default' => 'doctrine.dbal.default_connection',
                        'legacy' => 'doctrine.dbal.legacy_connection',
                    ]);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => [
                            'enabled' => true,
                            'exclude_connections' => ['legacy'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.dbal.middleware.default'));
        static::assertFalse($container->has('flow.telemetry.dbal.middleware.legacy'));
    }

    public function test_excluded_connections_by_regex_pattern_are_not_traced(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', [
                        'default' => 'doctrine.dbal.default_connection',
                        'debug_primary' => 'doctrine.dbal.debug_primary_connection',
                        'debug_secondary' => 'doctrine.dbal.debug_secondary_connection',
                    ]);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => [
                            'enabled' => true,
                            'exclude_connections' => ['/^debug_.*/'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.dbal.middleware.default'));
        static::assertFalse($container->has('flow.telemetry.dbal.middleware.debug_primary'));
        static::assertFalse($container->has('flow.telemetry.dbal.middleware.debug_secondary'));
    }

    public function test_excluded_table_queries_are_not_traced(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => [
                            'enabled' => true,
                            'exclude_tables' => ['cache_items'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingMiddleware $middleware */
        $middleware = $container->get('flow.telemetry.dbal.middleware.default');

        $configuration = new Configuration();
        $configuration->setMiddlewares([$middleware]);

        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ], $configuration);

        $connection->executeStatement('CREATE TABLE cache_items (item_id TEXT PRIMARY KEY, item_data TEXT)');
        $connection->executeStatement('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

        $statement = $connection->prepare('INSERT INTO cache_items (item_id, item_data) VALUES (?, ?)');
        $statement->bindValue(1, 'k');
        $statement->bindValue(2, 'v');
        $statement->executeStatement();

        $connection->executeQuery('SELECT * FROM users WHERE id = 1');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spanNames = array_map(static fn($s) => $s->name(), $processor->endedSpans());

        static::assertNotContains('INSERT cache_items', $spanNames);
        static::assertContains('SELECT users', $spanNames);

        foreach ($processor->endedSpans() as $span) {
            $queryText = $span->attributes()['db.query.text'] ?? null;

            if (is_string($queryText)) {
                static::assertStringNotContainsStringIgnoringCase('cache_items', $queryText);
            }
        }
    }

    public function test_exec_creates_span_with_sql_attribute(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => ['enabled' => true],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingMiddleware $middleware */
        $middleware = $container->get('flow.telemetry.dbal.middleware.default');

        $configuration = new Configuration();
        $configuration->setMiddlewares([$middleware]);

        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ], $configuration);

        $connection->executeStatement('CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $execSpan = null;

        foreach ($spans as $span) {
            if ($span->name() === 'CREATE') {
                $execSpan = $span;
            }
        }

        static::assertNotNull($execSpan, 'Exec span should exist');
        static::assertSame(SpanKind::CLIENT, $execSpan->kind());
        static::assertSame(
            'CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)',
            $execSpan->attributes()['db.query.text'],
        );
        static::assertNull($execSpan->status());
    }

    public function test_failed_transaction_creates_rollback_span(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => ['enabled' => true],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingMiddleware $middleware */
        $middleware = $container->get('flow.telemetry.dbal.middleware.default');

        $configuration = new Configuration();
        $configuration->setMiddlewares([$middleware]);

        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ], $configuration);

        $connection->executeStatement('CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)');

        $connection->beginTransaction();
        $connection->executeStatement('INSERT INTO test_table (name) VALUES ("test")');
        $connection->rollBack();

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $spanNames = array_map(static fn($s) => $s->name(), $spans);

        static::assertContains('BEGIN TRANSACTION', $spanNames);
        static::assertCount(1, array_filter($spanNames, static fn($n) => $n === 'BEGIN TRANSACTION'));

        $transactionSpan = null;

        foreach ($spans as $span) {
            if ($span->name() === 'BEGIN TRANSACTION') {
                $transactionSpan = $span;
            }
        }

        static::assertNotNull($transactionSpan, 'Transaction span should exist');
        static::assertSame(SpanKind::CLIENT, $transactionSpan->kind());
        static::assertNull($transactionSpan->status(), 'a clean rollBack leaves the transaction span status unset');
    }

    public function test_long_sql_is_truncated_when_max_length_configured(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => [
                            'enabled' => true,
                            'max_sql_length' => 20,
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingMiddleware $middleware */
        $middleware = $container->get('flow.telemetry.dbal.middleware.default');

        $configuration = new Configuration();
        $configuration->setMiddlewares([$middleware]);

        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ], $configuration);

        $connection->executeStatement(
            'CREATE TABLE test_table_with_very_long_name (id INTEGER PRIMARY KEY, name TEXT)',
        );
        $connection->executeQuery('SELECT * FROM test_table_with_very_long_name WHERE id = 1');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $querySpan = null;

        foreach ($spans as $span) {
            if ($span->name() === 'SELECT test_table_with_very_long_name') {
                $querySpan = $span;
            }
        }

        static::assertNotNull($querySpan, 'Query span should exist');

        $truncatedSql = $querySpan->attributes()['db.query.text'];
        static::assertSame('SELECT * FROM test_t...', $truncatedSql);
        static::assertSame(23, mb_strlen($truncatedSql));
    }

    public function test_prepared_statement_creates_prepare_and_execute_spans(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => ['enabled' => true],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingMiddleware $middleware */
        $middleware = $container->get('flow.telemetry.dbal.middleware.default');

        $configuration = new Configuration();
        $configuration->setMiddlewares([$middleware]);

        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ], $configuration);

        $connection->executeStatement('CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)');

        $stmt = $connection->prepare('INSERT INTO test_table (name) VALUES (:name)');
        $stmt->bindValue('name', 'test_value');
        $stmt->executeStatement();

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $spanNames = array_map(static fn($s) => $s->name(), $spans);

        static::assertContains('doctrine.dbal.connection', $spanNames);
        static::assertContains('CREATE', $spanNames);

        // prepare and execute of the same statement both carry the semconv "INSERT test_table" name
        $insertSpans = array_values(array_filter($spans, static fn($s) => $s->name() === 'INSERT test_table'));
        static::assertCount(2, $insertSpans, 'a prepare span and an execute span');

        foreach ($insertSpans as $insertSpan) {
            static::assertSame(SpanKind::CLIENT, $insertSpan->kind());
            static::assertSame(
                'INSERT INTO test_table (name) VALUES (:name)',
                $insertSpan->attributes()['db.query.text'],
            );
            static::assertNull($insertSpan->status());
        }
    }

    public function test_query_creates_span_with_sql_attribute(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => ['enabled' => true],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingMiddleware $middleware */
        $middleware = $container->get('flow.telemetry.dbal.middleware.default');

        $configuration = new Configuration();
        $configuration->setMiddlewares([$middleware]);

        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ], $configuration);

        $connection->executeQuery('SELECT 1 as value');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(2, $spans);

        $connectionSpan = $spans[0];
        static::assertSame('doctrine.dbal.connection', $connectionSpan->name());
        static::assertSame(SpanKind::CLIENT, $connectionSpan->kind());
        static::assertSame('default', $connectionSpan->attributes()['flow.db.connection.name']);
        static::assertNull($connectionSpan->status());

        $querySpan = $spans[1];
        static::assertSame('SELECT', $querySpan->name());
        static::assertSame(SpanKind::CLIENT, $querySpan->kind());
        static::assertSame('SELECT 1 as value', $querySpan->attributes()['db.query.text']);
        static::assertNull($querySpan->status());
    }

    public function test_query_error_creates_span_with_error_status(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => ['enabled' => true],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingMiddleware $middleware */
        $middleware = $container->get('flow.telemetry.dbal.middleware.default');

        $configuration = new Configuration();
        $configuration->setMiddlewares([$middleware]);

        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ], $configuration);

        $exceptionThrown = false;

        try {
            $connection->executeQuery('SELECT * FROM non_existent_table');
        } catch (Exception) {
            $exceptionThrown = true;
        }

        static::assertTrue($exceptionThrown, 'Expected exception was not thrown');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $querySpan = null;

        foreach ($spans as $span) {
            if ($span->name() === 'SELECT non_existent_table') {
                $querySpan = $span;
            }
        }

        static::assertNotNull($querySpan, 'Query span should exist');
        static::assertSame('SELECT * FROM non_existent_table', $querySpan->attributes()['db.query.text']);

        $status = $querySpan->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());

        $events = $querySpan->events();
        static::assertCount(1, $events);
        static::assertSame('exception', $events[0]->name());
    }

    public function test_transaction_creates_single_grouped_span_wrapping_queries(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'dbal' => ['enabled' => true],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TracingMiddleware $middleware */
        $middleware = $container->get('flow.telemetry.dbal.middleware.default');

        $configuration = new Configuration();
        $configuration->setMiddlewares([$middleware]);

        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ], $configuration);

        $connection->executeStatement('CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)');

        $connection->beginTransaction();
        $connection->executeStatement('INSERT INTO test_table (name) VALUES ("test")');
        $connection->commit();

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $spanNames = array_map(static fn($s) => $s->name(), $spans);

        static::assertContains('BEGIN TRANSACTION', $spanNames);
        static::assertCount(1, array_filter($spanNames, static fn($n) => $n === 'BEGIN TRANSACTION'));

        $transactionSpan = null;

        foreach ($spans as $span) {
            if ($span->name() === 'BEGIN TRANSACTION') {
                $transactionSpan = $span;
            }
        }

        static::assertNotNull($transactionSpan, 'Transaction span should exist');
        static::assertSame(SpanKind::CLIENT, $transactionSpan->kind());
        static::assertSame('sqlite', $transactionSpan->attributes()['db.system.name'] ?? null);
        static::assertNull($transactionSpan->status());

        $nested = array_filter(
            $spans,
            static fn($s) => $s->context()->parentSpanId?->toHex() === $transactionSpan->context()->spanId->toHex(),
        );
        static::assertNotEmpty($nested, 'the INSERT executed inside the transaction must nest under the grouped span');
    }
}
