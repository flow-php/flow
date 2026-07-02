<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\PathExclusionRule;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Controller\TestController;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Event\ControllerEvent;
use Symfony\Component\HttpKernel\HttpKernelInterface;
use Symfony\Component\HttpKernel\KernelEvents;
use Symfony\Component\Routing\Route;
use Symfony\Component\Routing\Router;

#[CoversClass(HttpKernelSpanSubscriber::class)]
#[CoversClass(PathExclusionRule::class)]
final class HttpKernelSpanSubscriberTest extends KernelTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_does_not_extract_context_when_propagation_disabled(): void
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
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'context_propagation' => false,
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

        $incomingTraceId = '0af7651916cd43dd8448eb211c80319c';
        $incomingSpanId = 'b7ad6b7169203331';
        $traceparent = "00-{$incomingTraceId}-{$incomingSpanId}-01";

        $request = Request::create('/test', 'GET');
        $request->headers->set('traceparent', $traceparent);
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        static::assertSame(200, $response->getStatusCode());

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(2, $spans);

        $span = array_values(array_filter($spans, static fn(Span $s): bool => $s->kind() === SpanKind::SERVER))[0];
        static::assertNotSame($incomingTraceId, $span->context()->traceId->toHex());
        static::assertNull($span->context()->parentSpanId);
    }

    public function test_does_not_trace_when_disabled(): void
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
                            'type' => 'memory',
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

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(0, $spans);
    }

    public function test_excludes_path_with_exact_match(): void
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
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'exclude_paths' => [
                                ['path' => '/_wdt'],
                            ],
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
        $routes->add('wdt', new Route('/_wdt', ['_controller' => TestController::class . '::index']));

        $request = Request::create('/test', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        $request = Request::create('/_wdt', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(2, $spans);
        $requestSpan = array_values(array_filter(
            $spans,
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER,
        ))[0];
        static::assertSame('GET /test', $requestSpan->name());
    }

    public function test_excludes_path_with_method_filter(): void
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
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'exclude_paths' => [
                                ['path' => '/_wdt', 'method' => 'GET'],
                            ],
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
        $routes->add(
            'wdt',
            new Route('/_wdt', ['_controller' => TestController::class . '::index'], [], [], '', [], ['GET', 'POST']),
        );

        $request = Request::create('/_wdt', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        $request = Request::create('/_wdt', 'POST');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(2, $spans);
        $requestSpan = array_values(array_filter(
            $spans,
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER,
        ))[0];
        static::assertSame('POST /_wdt', $requestSpan->name());
    }

    public function test_excludes_path_with_regex_pattern(): void
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
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'exclude_paths' => [
                                ['path' => '/^\/_profiler.*/'],
                            ],
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
        $routes->add('_profiler_search', new Route('/_profiler/search', [
            '_controller' => TestController::class . '::index',
        ]));

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

        static::assertCount(2, $spans);
        $requestSpan = array_values(array_filter(
            $spans,
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER,
        ))[0];
        static::assertSame('GET /test', $requestSpan->name());
    }

    public function test_extracts_context_from_traceparent_header(): void
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
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'context_propagation' => true,
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

        $incomingTraceId = '0af7651916cd43dd8448eb211c80319c';
        $incomingSpanId = 'b7ad6b7169203331';
        $traceparent = "00-{$incomingTraceId}-{$incomingSpanId}-01";

        $request = Request::create('/test', 'GET');
        $request->headers->set('traceparent', $traceparent);
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        static::assertSame(200, $response->getStatusCode());

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(2, $spans);

        $span = array_values(array_filter($spans, static fn(Span $s): bool => $s->kind() === SpanKind::SERVER))[0];
        static::assertSame($incomingTraceId, $span->context()->traceId->toHex());
        static::assertSame($incomingSpanId, $span->context()->parentSpanId?->toHex());
    }

    public function test_extracts_context_from_query_when_enabled(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'instrumentation' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'context_propagation' => true,
                            'context_propagation_query' => true,
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
        $router->getRouteCollection()->add('test_index', new Route('/test', [
            '_controller' => TestController::class . '::index',
        ]));

        $incomingTraceId = '0af7651916cd43dd8448eb211c80319c';
        $incomingSpanId = 'b7ad6b7169203331';

        $request = Request::create('/test?traceparent=00-' . $incomingTraceId . '-' . $incomingSpanId . '-01', 'GET');
        $kernel->terminate($request, $kernel->handle($request));

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $span = array_values(array_filter(
            $processor->endedSpans(),
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER,
        ))[0];

        static::assertSame($incomingTraceId, $span->context()->traceId->toHex());
        static::assertSame($incomingSpanId, $span->context()->parentSpanId?->toHex());
    }

    public function test_query_context_is_ignored_when_not_enabled(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => true, 'context_propagation' => true],
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

        $incomingTraceId = '0af7651916cd43dd8448eb211c80319c';

        $request = Request::create('/test?traceparent=00-' . $incomingTraceId . '-b7ad6b7169203331-01', 'GET');
        $kernel->terminate($request, $kernel->handle($request));

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $span = array_values(array_filter(
            $processor->endedSpans(),
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER,
        ))[0];

        static::assertNotSame($incomingTraceId, $span->context()->traceId->toHex());
        static::assertNull($span->context()->parentSpanId);
    }

    public function test_header_takes_precedence_over_query(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'instrumentation' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'context_propagation' => true,
                            'context_propagation_query' => true,
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
        $router->getRouteCollection()->add('test_index', new Route('/test', [
            '_controller' => TestController::class . '::index',
        ]));

        $headerTraceId = '0af7651916cd43dd8448eb211c80319c';
        $queryTraceId = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

        $request = Request::create('/test?traceparent=00-' . $queryTraceId . '-b7ad6b7169203331-01', 'GET');
        $request->headers->set('traceparent', '00-' . $headerTraceId . '-b7ad6b7169203331-01');
        $kernel->terminate($request, $kernel->handle($request));

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $span = array_values(array_filter(
            $processor->endedSpans(),
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER,
        ))[0];

        static::assertSame($headerTraceId, $span->context()->traceId->toHex());
    }

    public function test_extracted_context_does_not_leak_into_the_next_request(): void
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
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'context_propagation' => true,
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
        $router->getRouteCollection()->add('test_index', new Route('/test', [
            '_controller' => TestController::class . '::index',
        ]));

        $incomingTraceId = '0af7651916cd43dd8448eb211c80319c';

        $firstRequest = Request::create('/test', 'GET');
        $firstRequest->headers->set('traceparent', "00-{$incomingTraceId}-b7ad6b7169203331-01");
        $kernel->terminate($firstRequest, $kernel->handle($firstRequest));

        $secondRequest = Request::create('/test', 'GET');
        $kernel->terminate($secondRequest, $kernel->handle($secondRequest));

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $serverSpans = array_values(array_filter(
            $processor->endedSpans(),
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER,
        ));

        static::assertCount(2, $serverSpans);

        $continuingRemoteTrace = array_values(array_filter(
            $serverSpans,
            static fn(Span $s): bool => $s->context()->traceId->toHex() === $incomingTraceId,
        ));
        $freshTrace = array_values(array_filter(
            $serverSpans,
            static fn(Span $s): bool => $s->context()->traceId->toHex() !== $incomingTraceId,
        ));

        static::assertCount(
            1,
            $continuingRemoteTrace,
            'only the request carrying traceparent continues the remote trace',
        );
        static::assertCount(1, $freshTrace);
        static::assertNull(
            $freshTrace[0]->context()->parentSpanId,
            'a request without traceparent must start a fresh root trace, not inherit the previous request remote parent',
        );
    }

    public function test_handles_missing_trace_headers_gracefully(): void
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
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'context_propagation' => true,
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

        $request = Request::create('/test', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        static::assertSame(200, $response->getStatusCode());

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(2, $spans);

        $span = array_values(array_filter($spans, static fn(Span $s): bool => $s->kind() === SpanKind::SERVER))[0];
        static::assertNotEmpty($span->context()->traceId->toHex());
        static::assertNull($span->context()->parentSpanId);
    }

    public function test_client_error_status_leaves_server_span_status_unset(): void
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
                            'type' => 'memory',
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
        $routes->add('test_error', new Route('/error', ['_controller' => TestController::class . '::error']));

        $request = Request::create('/error', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        static::assertSame(404, $response->getStatusCode());

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(2, $spans);

        $span = array_values(array_filter($spans, static fn(Span $s): bool => $s->kind() === SpanKind::SERVER))[0];
        $attributes = $span->attributes();
        static::assertSame(404, $attributes['http.response.status_code']);
        static::assertArrayNotHasKey('error.type', $attributes);

        // OTEL semconv: a 4xx is the client's fault, so the SERVER span status stays unset.
        static::assertNull($span->status());
    }

    public function test_server_error_status_marks_server_span_as_error(): void
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
                            'type' => 'memory',
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
        $routes->add('test_server_error', new Route('/server-error', [
            '_controller' => TestController::class . '::serverError',
        ]));

        $request = Request::create('/server-error', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        static::assertSame(500, $response->getStatusCode());

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(2, $spans);

        $span = array_values(array_filter($spans, static fn(Span $s): bool => $s->kind() === SpanKind::SERVER))[0];
        $attributes = $span->attributes();
        static::assertSame(500, $attributes['http.response.status_code']);
        static::assertSame('500', $attributes['error.type']);

        $status = $span->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('HTTP 500', $status->description);
    }

    public function test_traces_successful_http_request(): void
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
                            'type' => 'memory',
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
        $kernel->terminate($request, $response);

        static::assertSame(200, $response->getStatusCode());

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(2, $spans);

        $span = array_values(array_filter($spans, static fn(Span $s): bool => $s->kind() === SpanKind::SERVER))[0];
        static::assertSame('GET /test', $span->name());
        static::assertSame(SpanKind::SERVER, $span->kind());

        $attributes = $span->attributes();
        static::assertSame('GET', $attributes['http.request.method']);
        static::assertSame(200, $attributes['http.response.status_code']);
        static::assertSame('/test', $attributes['http.route']);
        static::assertSame(TestController::class . '::index', $attributes['controller']);
    }

    public function test_span_name_falls_back_to_method_without_a_matched_route(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => true],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        $request = Request::create('/no-such-route', 'GET');
        $kernel->terminate($request, $kernel->handle($request));

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $span = array_values(array_filter(
            $processor->endedSpans(),
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER,
        ))[0];

        // OTEL semconv: no low-cardinality route, so the span name is just the method.
        static::assertSame('GET', $span->name());
        static::assertArrayNotHasKey('http.route', $span->attributes());
    }

    public function test_sub_request_without_a_route_is_named_after_the_controller(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => true],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        // A render(controller(...)) sub-request: controller preset, so routing is skipped and there is no _route.
        $subRequest = Request::create('/fragment', 'GET');
        $subRequest->attributes->set('_controller', TestController::class . '::index');
        $kernel->handle($subRequest, HttpKernelInterface::SUB_REQUEST);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $requestSpan = array_values(array_filter(
            $processor->endedSpans(),
            static fn(Span $s): bool => (
                $s->kind() === SpanKind::INTERNAL
                && $s->name() === 'GET ' . TestController::class . '::index'
            ),
        ));

        static::assertCount(1, $requestSpan);
        static::assertArrayNotHasKey('http.route', $requestSpan[0]->attributes());
    }

    public function test_route_naming_name_uses_the_symfony_route_name(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => true, 'route_naming' => 'name'],
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

        $request = Request::create('/test', 'GET');
        $kernel->terminate($request, $kernel->handle($request));

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $span = array_values(array_filter(
            $processor->endedSpans(),
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER,
        ))[0];

        static::assertSame('GET test_index', $span->name());
        static::assertSame('test_index', $span->attributes()['http.route']);
    }

    public function test_completes_sub_request_span_nested_under_main_request(): void
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
                            'type' => 'memory',
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

        // A real sub-request (render(controller(...))) is dispatched from within the main request, so
        // the kernel's request stack is non-empty and services are not reset between the two spans.
        // Issuing it as a separate top-level handle() would instead trip Symfony's services_resetter.
        /** @var EventDispatcherInterface $dispatcher */
        $dispatcher = $container->get('event_dispatcher');
        $dispatcher->addListener(
            KernelEvents::CONTROLLER,
            static function (ControllerEvent $event) use ($kernel): void {
                if (!$event->isMainRequest()) {
                    return;
                }

                $kernel->handle(Request::create('/test', 'GET'), HttpKernelInterface::SUB_REQUEST);
            },
            -100,
        );

        $mainRequest = Request::create('/test', 'GET');
        $response = $kernel->handle($mainRequest);

        $kernel->terminate($mainRequest, $response);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $serverSpan = array_values(array_filter(
            $spans,
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER && $s->name() === 'GET /test',
        ));
        $subRequestSpan = array_values(array_filter(
            $spans,
            static fn(Span $s): bool => $s->kind() === SpanKind::INTERNAL && $s->name() === 'GET /test',
        ));

        static::assertCount(1, $serverSpan);
        static::assertCount(
            1,
            $subRequestSpan,
            'sub-request request span must be completed and exported on kernel.finish_request',
        );

        static::assertSame('GET', $subRequestSpan[0]->attributes()['http.request.method']);
        static::assertTrue(
            $subRequestSpan[0]->context()->traceId->equals($serverSpan[0]->context()->traceId),
            'sub-request span must share the main request trace',
        );
        static::assertSame(
            $serverSpan[0]->context()->spanId->toHex(),
            $subRequestSpan[0]->context()->parentSpanId?->toHex(),
            'sub-request span must be a child of the main request span',
        );
    }

    public function test_terminate_phase_write_nests_under_request_span(): void
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
                            'type' => 'memory',
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

        // Emulate bindhq's PageListener::onKernelTerminate (priority 0): a synchronous DB write performed
        // on kernel.terminate, traced exactly like TracingConnection does (a flow.symfony.dbal CLIENT span).
        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');

        $activeSpanAtTerminate = false;

        /** @var EventDispatcherInterface $dispatcher */
        $dispatcher = $container->get('event_dispatcher');
        $dispatcher->addListener(
            KernelEvents::TERMINATE,
            static function () use ($telemetry, &$activeSpanAtTerminate): void {
                $tracer = $telemetry->tracer('flow.symfony.dbal', '0.0.0');
                $activeSpanAtTerminate = $tracer->activeSpan();
                $tracer->complete($tracer->span('doctrine.dbal.statement.prepare', SpanKind::CLIENT));
            },
            0,
        );

        $request = Request::create('/test', 'GET');
        $kernel->terminate($request, $kernel->handle($request));

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $serverSpan = array_values(array_filter(
            $spans,
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER,
        ))[0];
        $terminateSpan = array_values(array_filter(
            $spans,
            static fn(Span $s): bool => $s->name() === 'doctrine.dbal.statement.prepare',
        ))[0];

        static::assertNotNull(
            $activeSpanAtTerminate,
            'the request span must still be the active span at kernel.terminate priority 0',
        );
        static::assertTrue(
            $terminateSpan->context()->traceId->equals($serverSpan->context()->traceId),
            'a kernel.terminate DB write must nest under the request trace, not orphan into its own trace',
        );
        static::assertSame(
            $serverSpan->context()->spanId->toHex(),
            $terminateSpan->context()->parentSpanId?->toHex(),
            'the terminate-phase span must be a child of the request span',
        );
    }

    public function test_terminate_phase_write_nests_after_sub_request(): void
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
                            'type' => 'memory',
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

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');

        /** @var EventDispatcherInterface $dispatcher */
        $dispatcher = $container->get('event_dispatcher');

        // A render(controller(...)) sub-request dispatched from within the main request, exactly like the
        // existing sub-request test: it completes on kernel.finish_request and detaches its context scope.
        $dispatcher->addListener(
            KernelEvents::CONTROLLER,
            static function (ControllerEvent $event) use ($kernel): void {
                if (!$event->isMainRequest()) {
                    return;
                }

                $kernel->handle(Request::create('/test', 'GET'), HttpKernelInterface::SUB_REQUEST);
            },
            -100,
        );

        $activeSpanAtTerminate = false;
        $dispatcher->addListener(
            KernelEvents::TERMINATE,
            static function () use ($telemetry, &$activeSpanAtTerminate): void {
                $tracer = $telemetry->tracer('flow.symfony.dbal', '0.0.0');
                $activeSpanAtTerminate = $tracer->activeSpan();
                $tracer->complete($tracer->span('doctrine.dbal.statement.prepare', SpanKind::CLIENT));
            },
            0,
        );

        $mainRequest = Request::create('/test', 'GET');
        $kernel->terminate($mainRequest, $kernel->handle($mainRequest));

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        $serverSpan = array_values(array_filter(
            $spans,
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER,
        ))[0];
        $terminateSpan = array_values(array_filter(
            $spans,
            static fn(Span $s): bool => $s->name() === 'doctrine.dbal.statement.prepare',
        ))[0];

        static::assertNotNull(
            $activeSpanAtTerminate,
            'the request span must still be the active span at kernel.terminate after a sub-request',
        );
        static::assertTrue(
            $terminateSpan->context()->traceId->equals($serverSpan->context()->traceId),
            'a kernel.terminate write after a sub-request must nest under the request trace, not orphan',
        );
    }

    public function test_terminate_phase_write_is_suppressed_on_excluded_path(): void
    {
        // The bindhq orphan scenario: http_kernel excludes /_wdt (the web debug toolbar the browser fetches
        // after every dev page), so no request span is created. PageListener does NOT ignore /_wdt, so its
        // kernel.terminate system_events INSERT runs for the excluded request. Excluding a path now suppresses
        // tracing for the whole request, so that write (and any other DBAL/cache work) is dropped instead of
        // orphaning into its own root trace.
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
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'exclude_paths' => [
                                ['path' => '~^/_wdt~'],
                            ],
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
        $router->getRouteCollection()->add('wdt', new Route('/_wdt/{token}', [
            '_controller' => TestController::class . '::index',
        ]));

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');

        $suppressedAtTerminate = false;

        /** @var EventDispatcherInterface $dispatcher */
        $dispatcher = $container->get('event_dispatcher');
        $dispatcher->addListener(
            KernelEvents::TERMINATE,
            static function () use ($telemetry, &$suppressedAtTerminate): void {
                $tracer = $telemetry->tracer('flow.symfony.dbal', '0.0.0');
                $suppressedAtTerminate = $tracer->context()->isTracingSuppressed();
                $tracer->complete($tracer->span('doctrine.dbal.statement.prepare', SpanKind::CLIENT));
            },
            0,
        );

        $request = Request::create('/_wdt/abc123', 'GET');
        $kernel->terminate($request, $kernel->handle($request));

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertTrue(
            $suppressedAtTerminate,
            'tracing must be suppressed during the excluded request, including kernel.terminate',
        );
        static::assertCount(
            0,
            $spans,
            'an excluded path must produce no spans at all — not the request span, not orphan terminate-phase writes',
        );
    }

    public function test_injects_context_into_response_when_propagation_enabled(): void
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
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'context_propagation' => true,
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

        $request = Request::create('/test', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        static::assertSame(200, $response->getStatusCode());

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(2, $spans);

        $span = array_values(array_filter($spans, static fn(Span $s): bool => $s->kind() === SpanKind::SERVER))[0];
        static::assertSame(
            "00-{$span->context()->traceId->toHex()}-{$span->context()->spanId->toHex()}-01",
            $response->headers->get('traceparent'),
        );
    }

    public function test_does_not_inject_context_into_response_when_propagation_disabled(): void
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
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => [
                            'enabled' => true,
                            'context_propagation' => false,
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

        $request = Request::create('/test', 'GET');
        $response = $kernel->handle($request);
        $kernel->terminate($request, $response);

        static::assertSame(200, $response->getStatusCode());
        static::assertFalse($response->headers->has('traceparent'));
    }
}
