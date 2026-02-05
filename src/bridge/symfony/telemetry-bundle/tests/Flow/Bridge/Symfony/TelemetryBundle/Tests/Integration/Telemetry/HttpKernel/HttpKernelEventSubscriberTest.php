<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Telemetry\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Telemetry\HttpKernel\HttpKernelEventSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Controller\TestController;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Routing\{Route, Router};

#[CoversClass(HttpKernelEventSubscriber::class)]
final class HttpKernelEventSubscriberTest extends KernelTestCase
{
    protected function tearDown() : void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_does_not_trace_when_disabled() : void
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
                    'instances' => [
                        'default' => [
                            'tracer_provider' => [
                                'processor' => [
                                    'type' => 'memory',
                                    'exporter' => ['type' => 'memory'],
                                ],
                            ],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
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

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.default.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(0, $spans);
    }

    public function test_traces_http_request_with_error_status() : void
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
                    'instances' => [
                        'default' => [
                            'tracer_provider' => [
                                'processor' => [
                                    'type' => 'memory',
                                    'exporter' => ['type' => 'memory'],
                                ],
                            ],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => true,
                        'console' => false,
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var Router $router */
        $router = $container->get('router');
        $routes = $router->getRouteCollection();
        $routes->add('test_error', new Route('/error', ['_controller' => TestController::class . '::error']));

        $request = Request::create('/error', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        self::assertSame(404, $response->getStatusCode());

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.default.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        $attributes = $span->attributes();
        self::assertSame(404, $attributes['http.status_code']);

        $status = $span->status();
        self::assertNotNull($status);
        self::assertTrue($status->isError());
        self::assertSame('HTTP 404', $status->description);
    }

    public function test_traces_successful_http_request() : void
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
                    'instances' => [
                        'default' => [
                            'tracer_provider' => [
                                'processor' => [
                                    'type' => 'memory',
                                    'exporter' => ['type' => 'memory'],
                                ],
                            ],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => true,
                        'console' => false,
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

        self::assertSame(200, $response->getStatusCode());

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.default.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('GET test_index', $span->name());
        self::assertSame(SpanKind::SERVER, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('GET', $attributes['http.method']);
        self::assertSame(200, $attributes['http.status_code']);
        self::assertSame('test_index', $attributes['http.route']);
        self::assertSame(TestController::class . '::index', $attributes['code.function']);
    }
}
