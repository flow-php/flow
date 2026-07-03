<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Controller\TestController;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Routing\Route;
use Symfony\Component\Routing\Router;

#[CoversClass(HttpKernelFlushSubscriber::class)]
final class HttpKernelFlushSubscriberTest extends KernelTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_flush_is_called_on_terminate(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => [
                        'utf8' => true,
                        'resource' => __DIR__ . '/../../../Fixtures/config/routes.php',
                    ],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                            'batch_size' => 100,
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => true],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var Router $router */
        $router = $container->get('router');
        $routes = $router->getRouteCollection();
        $routes->add('test_index', new Route('/test', ['_controller' => TestController::class . '::index']));

        $request = Request::create('/test', 'GET');
        $response = $kernel->handle($request);

        /** @var MemoryExporter $exporter */
        $exporter = $container->get('flow.telemetry.exporter.memory');
        $spansBeforeTerminate = $exporter->spans();

        static::assertCount(0, $spansBeforeTerminate, 'Spans should not be exported before terminate (batching)');

        $kernel->terminate($request, $response);

        $spansAfterTerminate = $exporter->spans();

        static::assertCount(
            2,
            $spansAfterTerminate,
            'Request and controller spans should be exported after terminate when flush is called',
        );
    }

    public function test_repeated_request_cycles_on_the_same_kernel_keep_exporting(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => [
                        'utf8' => true,
                        'resource' => __DIR__ . '/../../../Fixtures/config/routes.php',
                    ],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                            'batch_size' => 100,
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => true],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var Router $router */
        $router = $container->get('router');
        $router->getRouteCollection()->add('test_index', new Route('/test', [
            '_controller' => TestController::class . '::index',
        ]));

        /** @var MemoryExporter $exporter */
        $exporter = $container->get('flow.telemetry.exporter.memory');

        $firstRequest = Request::create('/test', 'GET');
        $kernel->terminate($firstRequest, $kernel->handle($firstRequest));

        static::assertCount(2, $exporter->spans(), 'First worker request should flush its spans on terminate');

        $secondRequest = Request::create('/test', 'GET');
        $kernel->terminate($secondRequest, $kernel->handle($secondRequest));

        static::assertCount(
            4,
            $exporter->spans(),
            'A second request on the same kernel must still export; terminate must not shut telemetry down',
        );
    }

    public function test_flush_is_not_called_when_http_kernel_instrumentation_is_disabled(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => [
                        'utf8' => true,
                        'resource' => __DIR__ . '/../../../Fixtures/config/routes.php',
                    ],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                            'batch_size' => 100,
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var Router $router */
        $router = $container->get('router');
        $routes = $router->getRouteCollection();
        $routes->add('test_index', new Route('/test', ['_controller' => TestController::class . '::index']));

        $request = Request::create('/test', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        /** @var MemoryExporter $exporter */
        $exporter = $container->get('flow.telemetry.exporter.memory');
        $spans = $exporter->spans();

        static::assertCount(0, $spans, 'No spans should be exported when instrumentation is disabled');
    }
}
