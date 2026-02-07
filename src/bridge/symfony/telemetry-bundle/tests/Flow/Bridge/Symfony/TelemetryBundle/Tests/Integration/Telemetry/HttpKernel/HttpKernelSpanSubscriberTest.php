<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Telemetry\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Telemetry\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Controller\TestController;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Routing\{Route, Router};

#[CoversClass(HttpKernelSpanSubscriber::class)]
final class HttpKernelSpanSubscriberTest extends KernelTestCase
{
    #[\Override]
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
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
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

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(0, $spans);
    }

    public function test_excludes_route_with_exact_match() : void
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
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'telemetry' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'exclude_routes' => ['test_excluded'],
                        ],
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
        $routes->add('test_excluded', new Route('/excluded', ['_controller' => TestController::class . '::index']));

        $request = Request::create('/test', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        $request = Request::create('/excluded', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);
        self::assertSame('GET test_index', $spans[0]->name());
    }

    public function test_excludes_routes_with_regex_pattern() : void
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
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'telemetry' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'exclude_routes' => ['/^_profiler.*/'],
                        ],
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
        $routes->add('_profiler_home', new Route('/_profiler', ['_controller' => TestController::class . '::index']));
        $routes->add('_profiler_search', new Route('/_profiler/search', ['_controller' => TestController::class . '::index']));

        $request = Request::create('/test', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        $request = Request::create('/_profiler', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        $request = Request::create('/_profiler/search', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);
        self::assertSame('GET test_index', $spans[0]->name());
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
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
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
        $routes->add('test_error', new Route('/error', ['_controller' => TestController::class . '::error']));

        $request = Request::create('/error', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        self::assertSame(404, $response->getStatusCode());

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
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
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
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
        $kernel->terminate($request, $response);

        self::assertSame(200, $response->getStatusCode());

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('GET test_index', $span->name());
        self::assertSame(SpanKind::SERVER, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('GET', $attributes['http.method']);
        self::assertSame(200, $attributes['http.status_code']);
        self::assertSame('test_index', $attributes['http.route']);
        self::assertSame(TestController::class . '::index', $attributes['controller']);
    }
}
