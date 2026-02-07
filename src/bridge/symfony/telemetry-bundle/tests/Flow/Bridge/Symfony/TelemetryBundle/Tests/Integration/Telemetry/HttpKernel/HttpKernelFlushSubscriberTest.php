<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Telemetry\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Telemetry\HttpKernel\HttpKernelFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Controller\TestController;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanExporter;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Routing\{Route, Router};

#[CoversClass(HttpKernelFlushSubscriber::class)]
final class HttpKernelFlushSubscriberTest extends KernelTestCase
{
    #[\Override]
    protected function tearDown() : void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_flush_is_called_on_terminate() : void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
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
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                            'batch_size' => 100,
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'telemetry' => [
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

        /** @var MemorySpanExporter $exporter */
        $exporter = $container->get('flow.telemetry.tracer_provider.processor.exporter');
        $spansBeforeTerminate = $exporter->spans();

        self::assertCount(0, $spansBeforeTerminate, 'Spans should not be exported before terminate (batching)');

        $kernel->terminate($request, $response);

        $spansAfterTerminate = $exporter->spans();

        self::assertCount(1, $spansAfterTerminate, 'Spans should be exported after terminate when flush is called');
    }

    public function test_flush_is_not_called_when_http_kernel_instrumentation_is_disabled() : void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
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
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                            'batch_size' => 100,
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'telemetry' => [
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

        /** @var MemorySpanExporter $exporter */
        $exporter = $container->get('flow.telemetry.tracer_provider.processor.exporter');
        $spans = $exporter->spans();

        self::assertCount(0, $spans, 'No spans should be exported when instrumentation is disabled');
    }
}
