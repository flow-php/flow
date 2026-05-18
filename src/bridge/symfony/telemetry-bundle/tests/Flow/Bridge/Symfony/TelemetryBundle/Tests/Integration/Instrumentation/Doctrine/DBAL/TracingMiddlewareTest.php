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

use function array_map;
use function interface_exists;
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
        static::assertSame('default', $connectionSpan->attributes()['db.connection.name']);
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
                        'dbal' => ['enabled' => true, 'log_sql' => true],
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
            if ($span->name() === 'doctrine.dbal.connection.exec') {
                $execSpan = $span;
            }
        }

        static::assertNotNull($execSpan, 'Exec span should exist');
        static::assertSame(SpanKind::CLIENT, $execSpan->kind());
        static::assertSame(
            'CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)',
            $execSpan->attributes()['db.query.text'],
        );
        static::assertTrue($execSpan->status()?->isOk());
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
                        'dbal' => ['enabled' => true, 'log_sql' => true],
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

        static::assertContains('doctrine.dbal.transaction.begin', $spanNames);
        static::assertContains('doctrine.dbal.transaction.rollback', $spanNames);
        static::assertNotContains('doctrine.dbal.transaction.commit', $spanNames);

        $rollbackSpan = null;

        foreach ($spans as $span) {
            if ($span->name() === 'doctrine.dbal.transaction.rollback') {
                $rollbackSpan = $span;
            }
        }

        static::assertNotNull($rollbackSpan, 'Rollback span should exist');
        static::assertSame(SpanKind::CLIENT, $rollbackSpan->kind());
        static::assertTrue($rollbackSpan->status()?->isOk());
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
                            'log_sql' => true,
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
            if ($span->name() === 'doctrine.dbal.connection.query') {
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
                        'dbal' => ['enabled' => true, 'log_sql' => true],
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
        static::assertContains('doctrine.dbal.connection.exec', $spanNames);
        static::assertContains('doctrine.dbal.statement.prepare', $spanNames);
        static::assertContains('doctrine.dbal.statement.execute', $spanNames);

        $prepareSpan = null;
        $executeSpan = null;

        foreach ($spans as $span) {
            if ($span->name() === 'doctrine.dbal.statement.prepare') {
                $prepareSpan = $span;
            }

            if ($span->name() === 'doctrine.dbal.statement.execute') {
                $executeSpan = $span;
            }
        }

        static::assertNotNull($prepareSpan, 'Prepare span should exist');
        static::assertSame('INSERT INTO test_table (name) VALUES (:name)', $prepareSpan->attributes()['db.query.text']);
        static::assertTrue($prepareSpan->status()?->isOk());

        static::assertNotNull($executeSpan, 'Execute span should exist');
        static::assertTrue($executeSpan->status()?->isOk());
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
                        'dbal' => ['enabled' => true, 'log_sql' => true],
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
        static::assertSame('default', $connectionSpan->attributes()['db.connection.name']);
        static::assertTrue($connectionSpan->status()?->isOk());

        $querySpan = $spans[1];
        static::assertSame('doctrine.dbal.connection.query', $querySpan->name());
        static::assertSame(SpanKind::CLIENT, $querySpan->kind());
        static::assertSame('SELECT 1 as value', $querySpan->attributes()['db.query.text']);
        static::assertTrue($querySpan->status()?->isOk());
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
                        'dbal' => ['enabled' => true, 'log_sql' => true],
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
            if ($span->name() === 'doctrine.dbal.connection.query') {
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

    public function test_sql_not_logged_when_log_sql_disabled(): void
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
                            'log_sql' => false,
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

        $connection->executeQuery('SELECT 1 as value');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $querySpan = null;

        foreach ($spans as $span) {
            if ($span->name() === 'doctrine.dbal.connection.query') {
                $querySpan = $span;
            }
        }

        static::assertNotNull($querySpan, 'Query span should exist');
        static::assertArrayNotHasKey('db.query.text', $querySpan->attributes());
    }

    public function test_transaction_creates_begin_commit_spans(): void
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
                        'dbal' => ['enabled' => true, 'log_sql' => true],
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

        static::assertContains('doctrine.dbal.transaction.begin', $spanNames);
        static::assertContains('doctrine.dbal.transaction.commit', $spanNames);

        $beginSpan = null;
        $commitSpan = null;

        foreach ($spans as $span) {
            if ($span->name() === 'doctrine.dbal.transaction.begin') {
                $beginSpan = $span;
            }

            if ($span->name() === 'doctrine.dbal.transaction.commit') {
                $commitSpan = $span;
            }
        }

        static::assertNotNull($beginSpan, 'Begin transaction span should exist');
        static::assertSame(SpanKind::CLIENT, $beginSpan->kind());
        static::assertTrue($beginSpan->status()?->isOk());

        static::assertNotNull($commitSpan, 'Commit span should exist');
        static::assertSame(SpanKind::CLIENT, $commitSpan->kind());
        static::assertTrue($commitSpan->status()?->isOk());
    }
}
