<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Profiler;

use Flow\Bridge\Symfony\PostgreSqlBundle\FlowPostgreSqlBundle;
use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\FlowPostgreSqlDataCollector;
use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\ProfilerClient;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\Controller\QueryController;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\KernelTestCase;
use LogicException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\TwigBundle\TwigBundle;
use Symfony\Bundle\WebProfilerBundle\WebProfilerBundle;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Profiler\Profiler;
use Symfony\Component\Routing\Route;
use Symfony\Component\Routing\Router;

use function array_merge;
use function getenv;

#[CoversClass(FlowPostgreSqlBundle::class)]
#[CoversClass(FlowPostgreSqlDataCollector::class)]
final class FlowPostgreSqlProfilerTest extends KernelTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_collector_registered_and_client_decorated_when_enabled(): void
    {
        $container = $this->bootWithWebProfiler(['enabled' => true])->getContainer();

        static::assertTrue($container->has('flow.postgresql.profiler.collector'));
        static::assertInstanceOf(
            FlowPostgreSqlDataCollector::class,
            $container->get('flow.postgresql.profiler.collector'),
        );
        static::assertInstanceOf(ProfilerClient::class, $container->get('flow.postgresql.default.client'));
    }

    public function test_queries_are_recorded_during_http_request(): void
    {
        [$kernel, $token] = $this->runQueryRequest();

        /** @var Profiler $profiler */
        $profiler = $kernel->getContainer()->get('profiler');
        $profile = $profiler->loadProfile($token);
        static::assertNotNull($profile);
        $collector = $profile->getCollector('flow_postgresql');
        static::assertInstanceOf(FlowPostgreSqlDataCollector::class, $collector);

        static::assertSame(1, $collector->getQueryCount());
        $query = $collector->getQueries()['default'][0];
        static::assertStringContainsString('VALUES (1), (2), (3)', $query['statement']);
        static::assertSame([2], $query['parameters']);
        static::assertSame(2, $query['returnedRows']);
        static::assertFalse($query['failed']);
        static::assertSame('default', $query['connection']);
        static::assertNotNull($query['caller']);
    }

    public function test_failed_query_is_recorded_during_http_request(): void
    {
        [$kernel, $token, $response] = $this->runQueryRequest('/failing-query', 'runFailing');

        /** @var Profiler $profiler */
        $profiler = $kernel->getContainer()->get('profiler');
        $profile = $profiler->loadProfile($token);
        static::assertNotNull($profile);
        $collector = $profile->getCollector('flow_postgresql');
        static::assertInstanceOf(FlowPostgreSqlDataCollector::class, $collector);

        static::assertSame(500, $response->getStatusCode());
        static::assertSame(1, $collector->getFailedCount());
        static::assertTrue($collector->getQueries()['default'][0]['failed']);
    }

    public function test_collector_not_registered_when_disabled(): void
    {
        $container = $this->bootWithWebProfiler(['enabled' => false])->getContainer();

        static::assertFalse($container->has('flow.postgresql.profiler.collector'));
        static::assertNotInstanceOf(ProfilerClient::class, $container->get('flow.postgresql.default.client'));
    }

    public function test_connection_can_opt_out_while_profiler_enabled(): void
    {
        $kernel = $this->bootKernel([
            'config' => function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestBundle(TwigBundle::class);
                $kernel->addTestBundle(WebProfilerBundle::class);
                $kernel->addTestExtensionConfig('framework', array_merge($this->frameworkConfig(), [
                    'profiler' => ['enabled' => true, 'collect' => true],
                ]));
                $kernel->addTestExtensionConfig('twig', ['debug' => true, 'strict_variables' => false]);
                $kernel->addTestExtensionConfig('web_profiler', ['toolbar' => false, 'intercept_redirects' => false]);
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => getenv('PGSQL_DATABASE_URL')
                                ?: 'postgresql://postgres:postgres@127.0.0.1:5452/postgres',
                            'profiler' => false,
                        ],
                    ],
                    'profiler' => ['enabled' => true],
                ]);
            },
        ]);
        $container = $kernel->getContainer();

        // Panel still registered (profiler enabled) but the opted-out connection is not decorated.
        static::assertTrue($container->has('flow.postgresql.profiler.collector'));
        static::assertNotInstanceOf(ProfilerClient::class, $container->get('flow.postgresql.default.client'));
    }

    public function test_auto_disabled_when_web_profiler_bundle_absent(): void
    {
        $container = $this->bootWithoutWebProfiler([])->getContainer();

        static::assertFalse($container->has('flow.postgresql.profiler.collector'));
    }

    public function test_forcing_enabled_without_web_profiler_bundle_throws(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('symfony/web-profiler-bundle is not registered');

        $this->bootWithoutWebProfiler(['enabled' => true]);
    }

    public function test_panel_renders_executed_queries_in_the_profiler(): void
    {
        [$kernel, $token] = $this->runQueryRequest();

        $panel = $kernel->handle(Request::create('/_profiler/' . $token . '?panel=flow_postgresql'));
        $html = (string) $panel->getContent();

        static::assertSame(200, $panel->getStatusCode());
        static::assertStringContainsString('Flow PostgreSQL', $html);
        static::assertStringContainsString('VALUES (1), (2), (3)', $html);
        static::assertStringContainsString('Explain query', $html);
    }

    public function test_explain_endpoint_renders_the_plan(): void
    {
        [$kernel, $token] = $this->runQueryRequest();

        $explain = $kernel->handle(Request::create(
            '/_profiler/' . $token . '?panel=flow_postgresql&page=explain&connection=default&query=0',
        ));

        static::assertSame(200, $explain->getStatusCode());
        static::assertStringContainsString('cost=', (string) $explain->getContent());
    }

    public function test_explain_endpoint_rejects_unknown_query(): void
    {
        [$kernel, $token] = $this->runQueryRequest();

        $explain = $kernel->handle(Request::create(
            '/_profiler/' . $token . '?panel=flow_postgresql&page=explain&connection=default&query=99',
        ));

        static::assertStringContainsString('does not exist', (string) $explain->getContent());
    }

    /**
     * @return array{0: TestKernel, 1: string, 2: Response}
     */
    private function runQueryRequest(string $path = '/query', string $action = 'run'): array
    {
        $kernel = $this->bootForPanelRendering();
        $container = $kernel->getContainer();

        /** @var Router $router */
        $router = $container->get('router');
        $router->getRouteCollection()->add('query', new Route($path, [
            '_controller' => QueryController::class . '::' . $action,
        ]));

        $request = Request::create($path, 'GET');
        $response = $kernel->handle($request);
        $token = (string) $response->headers->get('X-Debug-Token');
        $kernel->terminate($request, $response);

        return [$kernel, $token, $response];
    }

    /**
     * @param array{enabled?: bool|null, include_parameters?: bool} $profilerConfig
     */
    private function bootWithWebProfiler(array $profilerConfig): TestKernel
    {
        return $this->bootKernel([
            'config' => function (TestKernel $kernel) use ($profilerConfig): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestBundle(TwigBundle::class);
                $kernel->addTestBundle(WebProfilerBundle::class);
                $kernel->addTestExtensionConfig('framework', array_merge($this->frameworkConfig(), [
                    'profiler' => ['enabled' => true, 'collect' => true],
                ]));
                $kernel->addTestExtensionConfig('twig', ['debug' => true, 'strict_variables' => false]);
                $kernel->addTestExtensionConfig('web_profiler', ['toolbar' => false, 'intercept_redirects' => false]);
                $kernel->addTestExtensionConfig('flow_postgresql', $this->flowConfig($profilerConfig));
            },
        ]);
    }

    /**
     * @param array{enabled?: bool|null, include_parameters?: bool} $profilerConfig
     */
    private function bootWithoutWebProfiler(array $profilerConfig): TestKernel
    {
        return $this->bootKernel([
            'config' => function (TestKernel $kernel) use ($profilerConfig): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', $this->frameworkConfig());
                $kernel->addTestExtensionConfig('flow_postgresql', $this->flowConfig($profilerConfig));
            },
        ]);
    }

    private function bootForPanelRendering(): TestKernel
    {
        return $this->bootKernel([
            'config' => function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestBundle(TwigBundle::class);
                $kernel->addTestBundle(WebProfilerBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => [
                        'utf8' => true,
                        'resource' => __DIR__ . '/../../Fixtures/config/profiler_panel_routes.php',
                    ],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                    'profiler' => ['enabled' => true, 'collect' => true],
                ]);
                $kernel->addTestExtensionConfig('twig', ['debug' => true, 'strict_variables' => false]);
                $kernel->addTestExtensionConfig('web_profiler', ['toolbar' => false, 'intercept_redirects' => false]);
                $kernel->addTestExtensionConfig('flow_postgresql', $this->flowConfig(['enabled' => true]));
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $controller = new Definition(QueryController::class, [new Reference(
                        'flow.postgresql.default.client',
                    )]);
                    $controller->setPublic(true);
                    $controller->addTag('controller.service_arguments');
                    $container->setDefinition(QueryController::class, $controller);

                    $container->addCompilerPass(
                        new class implements CompilerPassInterface {
                            public function process(ContainerBuilder $container): void
                            {
                                if ($container->hasDefinition('profiler')) {
                                    $container->getDefinition('profiler')->setPublic(true);
                                }
                            }
                        },
                        PassConfig::TYPE_BEFORE_OPTIMIZATION,
                        -256,
                    );
                });
            },
        ]);
    }

    /**
     * @return array<string, mixed>
     */
    private function frameworkConfig(): array
    {
        return [
            'http_method_override' => false,
            'handle_all_throwables' => true,
            'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../Fixtures/config/profiler_panel_routes.php'],
        ];
    }

    /**
     * @param array{enabled?: bool|null, include_parameters?: bool} $profilerConfig
     *
     * @return array<string, mixed>
     */
    private function flowConfig(array $profilerConfig): array
    {
        return [
            'connections' => [
                'default' => [
                    'dsn' => getenv('PGSQL_DATABASE_URL') ?: 'postgresql://postgres:postgres@127.0.0.1:5452/postgres',
                ],
            ],
            'profiler' => $profilerConfig,
        ];
    }
}
