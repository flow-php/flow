<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Profiler;

use Flow\Bridge\Symfony\PostgreSqlBundle\FlowPostgreSqlBundle;
use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\FlowMigrationsDataCollector;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\Controller\QueryController;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\SimpleTestCatalogProvider;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\KernelTestCase;
use Flow\PostgreSql\Client\DsnParser;
use Flow\PostgreSql\Client\Infrastructure\PgSql\PgSqlClient;
use Flow\PostgreSql\Migrations\Migrator;
use Flow\PostgreSql\Migrations\Version;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\TwigBundle\TwigBundle;
use Symfony\Bundle\WebProfilerBundle\WebProfilerBundle;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Route;
use Symfony\Component\Routing\Router;

use function array_filter;
use function array_values;
use function Flow\Types\DSL\type_instance_of;
use function getenv;

#[CoversClass(FlowPostgreSqlBundle::class)]
#[CoversClass(FlowMigrationsDataCollector::class)]
final class FlowMigrationsProfilerTest extends KernelTestCase
{
    private const string FIXTURE_MIGRATIONS_DIR = __DIR__ . '/../../Fixture/migrations';

    private const string STORE_TABLE = 'flow_migrations_profiler_test';

    #[Override]
    protected function tearDown(): void
    {
        $client = PgSqlClient::connect((new DsnParser())->parse($this->dsn()));
        $client->execute('DROP TABLE IF EXISTS users, ' . self::STORE_TABLE . ' CASCADE');
        $client->close();

        restore_exception_handler();
        parent::tearDown();
    }

    public function test_collector_registered_when_migrations_and_profiler_enabled(): void
    {
        $container = $this->boot(['enabled' => true])->getContainer();

        static::assertTrue($container->has('flow.postgresql.profiler.migrations_collector'));
        static::assertInstanceOf(
            FlowMigrationsDataCollector::class,
            $container->get('flow.postgresql.profiler.migrations_collector'),
        );
    }

    public function test_collector_not_registered_when_toggle_disabled(): void
    {
        $container = $this->boot(['enabled' => true, 'migrations' => false])->getContainer();

        static::assertFalse($container->has('flow.postgresql.profiler.migrations_collector'));
    }

    public function test_collector_not_registered_when_migrations_disabled(): void
    {
        $container = $this->boot(['enabled' => true], migrationsEnabled: false)->getContainer();

        static::assertFalse($container->has('flow.postgresql.profiler.migrations_collector'));
    }

    public function test_collect_reports_executed_and_pending_with_execution_time(): void
    {
        $container = $this->boot(['enabled' => true])->getContainer();

        type_instance_of(Migrator::class)
            ->assert($container->get('flow.postgresql.default.migrations.migrator'))
            ->migrate(Version::fromString('20260401120000'));

        $collector = type_instance_of(FlowMigrationsDataCollector::class)->assert($container->get(
            'flow.postgresql.profiler.migrations_collector',
        ));
        $collector->collect(new Request(), new Response());

        static::assertSame(1, $collector->getExecutedCount());
        static::assertGreaterThan(0, $collector->getPendingCount());

        $connection = $collector->getConnections()['default'];
        static::assertNull($connection['error']);
        $configuration = $connection['configuration'];
        static::assertIsArray($configuration);
        static::assertSame(self::STORE_TABLE, $configuration['tableName']);

        $executed = array_values(array_filter(
            $connection['migrations'],
            static fn(array $migration): bool => $migration['state'] === 'executed',
        ));

        static::assertCount(1, $executed);
        static::assertSame('20260401120000', $executed[0]['version']);
        static::assertNotNull($executed[0]['executedAt']);
        static::assertNotNull($executed[0]['executionTimeMs']);
    }

    public function test_toolbar_and_panel_render_without_error(): void
    {
        $kernel = $this->boot(['enabled' => true]);
        $container = $kernel->getContainer();

        type_instance_of(Migrator::class)
            ->assert($container->get('flow.postgresql.default.migrations.migrator'))
            ->migrate(Version::fromString('20260401120000'));

        type_instance_of(Router::class)
            ->assert($container->get('router'))
            ->getRouteCollection()
            ->add('probe', new Route('/probe', ['_controller' => QueryController::class . '::run']));

        $request = Request::create('/probe', 'GET');
        $response = $kernel->handle($request);
        $token = (string) $response->headers->get('X-Debug-Token');
        $kernel->terminate($request, $response);

        // A Twig error in the toolbar/menu blocks would surface as a 500 here.
        $toolbar = $kernel->handle(Request::create('/_wdt/' . $token));
        static::assertSame(200, $toolbar->getStatusCode());
        static::assertStringContainsString('M12 2 2 7l10 5', (string) $toolbar->getContent());

        $panel = $kernel->handle(Request::create('/_profiler/' . $token . '?panel=flow_postgresql_migrations'));
        $html = (string) $panel->getContent();

        static::assertSame(200, $panel->getStatusCode());
        static::assertStringContainsString('Flow PostgreSQL Migrations', $html);
        static::assertStringContainsString('Flow Migrations', $html);
        static::assertStringContainsString('20260401120000', $html);
        static::assertStringContainsString('status-success', $html);
        static::assertStringContainsString('status-warning', $html);
    }

    /**
     * @param array{enabled?: bool|null, include_parameters?: bool, migrations?: bool} $profilerConfig
     */
    private function boot(array $profilerConfig, bool $migrationsEnabled = true): TestKernel
    {
        return $this->bootKernel([
            'config' => function (TestKernel $kernel) use ($profilerConfig, $migrationsEnabled): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestBundle(TwigBundle::class);
                $kernel->addTestBundle(WebProfilerBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                    'router' => [
                        'utf8' => true,
                        'resource' => __DIR__ . '/../../Fixtures/config/profiler_panel_routes.php',
                    ],
                    'profiler' => ['enabled' => true, 'collect' => true],
                ]);
                $kernel->addTestExtensionConfig('twig', ['debug' => true, 'strict_variables' => false]);
                $kernel->addTestExtensionConfig('web_profiler', ['toolbar' => true, 'intercept_redirects' => false]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('test.catalog_provider', SimpleTestCatalogProvider::class)->setPublic(true);

                    $controller = new Definition(QueryController::class, [new Reference(
                        'flow.postgresql.default.client',
                    )]);
                    $controller->setPublic(true);
                    $controller->addTag('controller.service_arguments');
                    $container->setDefinition(QueryController::class, $controller);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => ['default' => ['dsn' => $this->dsn()]],
                    'migrations' => [
                        'enabled' => $migrationsEnabled,
                        'directory' => self::FIXTURE_MIGRATIONS_DIR,
                        'namespace' => 'FlowTest\\Migrations',
                        'table_name' => self::STORE_TABLE,
                    ],
                    'catalog_providers' => [['catalog_provider_id' => 'test.catalog_provider']],
                    'profiler' => $profilerConfig,
                ]);
            },
        ]);
    }

    private function dsn(): string
    {
        return getenv('PGSQL_DATABASE_URL') ?: 'postgresql://postgres:postgres@127.0.0.1:5452/postgres';
    }
}
