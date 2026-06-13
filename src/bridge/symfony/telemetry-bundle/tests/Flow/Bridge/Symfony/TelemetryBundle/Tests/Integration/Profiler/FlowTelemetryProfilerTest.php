<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Profiler;

use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Bridge\Symfony\TelemetryBundle\FlowTelemetryBundle;
use Flow\Bridge\Symfony\TelemetryBundle\Profiler\FlowTelemetryDataCollector;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Controller\TestController;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Composite\CompositeExporter;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\TwigBundle\TwigBundle;
use Symfony\Bundle\WebProfilerBundle\WebProfilerBundle;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Profiler\Profiler;
use Symfony\Component\Routing\Route;
use Symfony\Component\Routing\Router;

use function array_merge;

#[CoversClass(FlowTelemetryBundle::class)]
#[CoversClass(FlowTelemetryDataCollector::class)]
final class FlowTelemetryProfilerTest extends KernelTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_collector_is_registered_with_the_profiler_when_enabled(): void
    {
        $container = $this->bootWithWebProfiler(['enabled' => true])->getContainer();

        /** @var Profiler $profiler */
        $profiler = $container->get('profiler');

        static::assertTrue($profiler->has('flow_telemetry'));
        static::assertInstanceOf(FlowTelemetryDataCollector::class, $profiler->get('flow_telemetry'));
        static::assertInstanceOf(MemoryExporter::class, $container->get('flow.telemetry.profiler.store'));
    }

    public function test_captured_exporter_is_decorated_and_tees_into_the_store(): void
    {
        $container = $this->bootWithWebProfiler(['enabled' => true])->getContainer();

        $exporter = $container->get('flow.telemetry.exporter.memory');
        static::assertInstanceOf(CompositeExporter::class, $exporter);

        /** @var MemoryExporter $store */
        $store = $container->get('flow.telemetry.profiler.store');
        $exporter->export(Signals::traces([SpanMother::withName('decorated-span')]));

        static::assertCount(1, $store->spans());
    }

    public function test_collector_is_not_registered_when_disabled(): void
    {
        $container = $this->bootWithWebProfiler(['enabled' => false])->getContainer();

        static::assertFalse($container->has('flow.telemetry.profiler.collector'));
        static::assertFalse($container->has('flow.telemetry.profiler.store'));
        static::assertInstanceOf(MemoryExporter::class, $container->get('flow.telemetry.exporter.memory'));
    }

    public function test_auto_enables_when_web_profiler_bundle_is_registered(): void
    {
        $container = $this->bootWithWebProfiler([])->getContainer();

        static::assertTrue($container->has('flow.telemetry.profiler.collector'));
    }

    public function test_auto_disabled_when_web_profiler_bundle_is_absent(): void
    {
        $container = $this->bootWithoutWebProfiler([])->getContainer();

        static::assertFalse($container->has('flow.telemetry.profiler.collector'));
    }

    public function test_forcing_enabled_without_web_profiler_bundle_throws(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Profiler integration requires symfony/web-profiler-bundle');

        $this->bootWithoutWebProfiler(['enabled' => true]);
    }

    public function test_panel_renders_captured_spans_in_the_profiler(): void
    {
        $kernel = $this->bootForPanelRendering();
        $container = $kernel->getContainer();

        /** @var Router $router */
        $router = $container->get('router');
        $router->getRouteCollection()->add('test_index', new Route('/test', [
            '_controller' => TestController::class . '::index',
        ]));

        // Emit a span that ends before the request handles, so it is flushed into the store at
        // profiler-save time (the http_kernel root span ends later and is intentionally not captured).
        /** @var Telemetry $telemetry */
        $telemetry = $container->get(Telemetry::class);
        $telemetry->tracer('app')->trace('controller_work', static fn(): ?string => null);

        $request = Request::create('/test', 'GET');
        $response = $kernel->handle($request);
        $token = $response->headers->get('X-Debug-Token');
        $kernel->terminate($request, $response);

        static::assertNotNull($token, 'The request must produce a profiler token');

        $panel = $kernel->handle(Request::create('/_profiler/' . $token . '?panel=flow_telemetry'));
        $html = (string) $panel->getContent();

        static::assertSame(200, $panel->getStatusCode());
        static::assertStringContainsString('Flow Telemetry', $html);
        static::assertStringContainsString('controller_work', $html);
        static::assertStringContainsString('Timeline', $html);
    }

    public function test_spans_are_captured_after_a_request(): void
    {
        $kernel = $this->bootWithWebProfiler(['enabled' => true]);
        $container = $kernel->getContainer();

        /** @var Router $router */
        $router = $container->get('router');
        $router->getRouteCollection()->add('test_index', new Route('/test', [
            '_controller' => TestController::class . '::index',
        ]));

        $request = Request::create('/test', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        /** @var FlowTelemetryDataCollector $collector */
        $collector = $container->get('flow.telemetry.profiler.collector');
        $collector->lateCollect();

        static::assertGreaterThanOrEqual(1, $collector->getSpanCount());
    }

    public function test_capture_logs_and_composite_processor_exporters_are_decorated(): void
    {
        $kernel = $this->bootKernel([
            'config' => function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestBundle(TwigBundle::class);
                $kernel->addTestBundle(WebProfilerBundle::class);
                $kernel->addTestExtensionConfig('framework', array_merge($this->frameworkConfig(), [
                    'profiler' => ['enabled' => true, 'collect' => true],
                ]));
                $kernel->addTestExtensionConfig('twig', ['debug' => true, 'strict_variables' => true]);
                $kernel->addTestExtensionConfig('web_profiler', ['toolbar' => false, 'intercept_redirects' => false]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'traces_a' => ['memory' => null],
                        'traces_b' => ['memory' => null],
                        'app_logs' => ['memory' => null],
                    ],
                    // Composite processor exercises the recursive exporter-id collection.
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'composite',
                            'processors' => [
                                ['type' => 'batching', 'exporter' => 'traces_a'],
                                ['type' => 'batching', 'exporter' => 'traces_b'],
                            ],
                        ],
                    ],
                    'logger_provider' => [
                        'processor' => ['type' => 'batching', 'exporter' => 'app_logs'],
                    ],
                    'profiler' => ['enabled' => true, 'capture_logs' => true],
                ]);
                $this->publishProfilerService($kernel);
            },
        ]);
        $container = $kernel->getContainer();

        static::assertInstanceOf(CompositeExporter::class, $container->get('flow.telemetry.exporter.traces_a'));
        static::assertInstanceOf(CompositeExporter::class, $container->get('flow.telemetry.exporter.traces_b'));
        static::assertInstanceOf(CompositeExporter::class, $container->get('flow.telemetry.exporter.app_logs'));
    }

    /**
     * @param array{enabled?: bool|null, capture_logs?: bool} $profilerConfig
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
                $kernel->addTestExtensionConfig('twig', ['debug' => true, 'strict_variables' => true]);
                $kernel->addTestExtensionConfig('web_profiler', ['toolbar' => false, 'intercept_redirects' => false]);
                $kernel->addTestExtensionConfig('flow_telemetry', $this->flowConfig($profilerConfig));
                $this->publishProfilerService($kernel);
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
                $kernel->addTestExtensionConfig('flow_telemetry', $this->flowConfig(['enabled' => true]));
                $this->publishProfilerService($kernel);
            },
        ]);
    }

    /**
     * @param array{enabled?: bool|null, capture_logs?: bool} $profilerConfig
     */
    private function bootWithoutWebProfiler(array $profilerConfig): TestKernel
    {
        return $this->bootKernel([
            'config' => function (TestKernel $kernel) use ($profilerConfig): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', $this->frameworkConfig());
                $kernel->addTestExtensionConfig('flow_telemetry', $this->flowConfig($profilerConfig));
            },
        ]);
    }

    /**
     * @return array<string, mixed>
     */
    private function frameworkConfig(): array
    {
        return [
            'router' => [
                'utf8' => true,
                'resource' => __DIR__ . '/../../Fixtures/config/routes.php',
            ],
            'http_method_override' => false,
            'handle_all_throwables' => true,
        ];
    }

    /**
     * @param array{enabled?: bool|null, capture_logs?: bool} $profilerConfig
     *
     * @return array<string, mixed>
     */
    private function flowConfig(array $profilerConfig): array
    {
        return [
            'resource' => [],
            'exporters' => ['memory' => ['memory' => null]],
            'tracer_provider' => [
                'processor' => ['type' => 'batching', 'batch_size' => 100, 'exporter' => 'memory'],
            ],
            'profiler' => $profilerConfig,
            'instrumentation' => [
                'http_kernel' => ['enabled' => true],
                'console' => ['enabled' => false],
                'messenger' => false,
            ],
        ];
    }

    private function publishProfilerService(TestKernel $kernel): void
    {
        $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
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
    }
}
