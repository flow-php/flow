<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Telemetry\HttpClient;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\HttpClientTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Telemetry\HttpClient\TracableHttpClient;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient\{FailingHttpClient, SuccessHttpClient};
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Contracts\HttpClient\HttpClientInterface;

#[CoversClass(TracableHttpClient::class)]
#[CoversClass(HttpClientTelemetryPass::class)]
final class TracableHttpClientTest extends KernelTestCase
{
    protected function setUp() : void
    {
        if (!\interface_exists(HttpClientInterface::class)) {
            self::markTestSkipped('symfony/http-client-contracts is not installed');
        }

        parent::setUp();
    }

    public function test_all_tagged_clients_are_wrapped_with_telemetry_decorator() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.api_client', SuccessHttpClient::class)
                        ->addArgument(200)
                        ->addTag('http_client.client')
                        ->setPublic(true);

                    $container->register('test.internal_client', SuccessHttpClient::class)
                        ->addArgument(200)
                        ->addTag('http_client.client')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'http_client' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(TracableHttpClient::class, $container->get('test.api_client'));
        self::assertInstanceOf(TracableHttpClient::class, $container->get('test.internal_client'));
    }

    public function test_decorator_not_registered_when_feature_disabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.api_client', SuccessHttpClient::class)
                        ->addArgument(200)
                        ->addTag('http_client.client')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instrumentation' => [
                        'http_client' => false,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(SuccessHttpClient::class, $container->get('test.api_client'));
    }

    public function test_excluded_client_by_exact_id_is_not_wrapped() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.api_client', SuccessHttpClient::class)
                        ->addArgument(200)
                        ->addTag('http_client.client')
                        ->setPublic(true);

                    $container->register('test.internal_client', SuccessHttpClient::class)
                        ->addArgument(200)
                        ->addTag('http_client.client')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'http_client' => [
                            'enabled' => true,
                            'exclude_clients' => ['test.internal_client'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(TracableHttpClient::class, $container->get('test.api_client'));
        self::assertInstanceOf(SuccessHttpClient::class, $container->get('test.internal_client'));
    }

    public function test_excluded_clients_by_regex_pattern_are_not_wrapped() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.api_client', SuccessHttpClient::class)
                        ->addArgument(200)
                        ->addTag('http_client.client')
                        ->setPublic(true);

                    $container->register('test.debug.client', SuccessHttpClient::class)
                        ->addArgument(200)
                        ->addTag('http_client.client')
                        ->setPublic(true);

                    $container->register('test.debug.another', SuccessHttpClient::class)
                        ->addArgument(200)
                        ->addTag('http_client.client')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'http_client' => [
                            'enabled' => true,
                            'exclude_clients' => ['/^test\\.debug\\..*/'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(TracableHttpClient::class, $container->get('test.api_client'));
        self::assertInstanceOf(SuccessHttpClient::class, $container->get('test.debug.client'));
        self::assertInstanceOf(SuccessHttpClient::class, $container->get('test.debug.another'));
    }

    public function test_wrapped_client_creates_error_span_for_http_error_response() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.error_client', SuccessHttpClient::class)
                        ->addArgument(500)
                        ->addTag('http_client.client')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'http_client' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var HttpClientInterface $client */
        $client = $container->get('test.error_client');
        $client->request('POST', 'http://localhost:8080/api/data');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('POST localhost', $span->name());
        self::assertSame(500, $span->attributes()['http.status_code']);

        $status = $span->status();
        self::assertNotNull($status);
        self::assertTrue($status->isError());
        self::assertSame('HTTP 500', $status->description);
    }

    public function test_wrapped_client_creates_span_with_correct_attributes() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.api_client', SuccessHttpClient::class)
                        ->addArgument(200)
                        ->addTag('http_client.client')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'http_client' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var HttpClientInterface $client */
        $client = $container->get('test.api_client');
        $client->request('GET', 'https://api.example.com/users?page=1');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('GET api.example.com', $span->name());
        self::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('GET', $attributes['http.method']);
        self::assertSame('https://api.example.com/users?page=1', $attributes['http.url']);
        self::assertSame('https', $attributes['http.scheme']);
        self::assertSame('api.example.com', $attributes['http.host']);
        self::assertSame('test.api_client', $attributes['http.client.name']);
        self::assertSame(200, $attributes['http.status_code']);

        $status = $span->status();
        self::assertNotNull($status);
        self::assertTrue($status->isOk());
    }

    public function test_wrapped_client_records_exception_and_creates_error_span() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.failing_client', FailingHttpClient::class)
                        ->addArgument('Connection refused')
                        ->addTag('http_client.client')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'http_client' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var HttpClientInterface $client */
        $client = $container->get('test.failing_client');

        $exceptionThrown = false;

        try {
            $client->request('GET', 'https://unreachable.example.com/');
        } catch (\RuntimeException $e) {
            $exceptionThrown = true;
            self::assertSame('Connection refused', $e->getMessage());
        }

        self::assertTrue($exceptionThrown, 'Expected exception was not thrown');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('GET unreachable.example.com', $span->name());

        $status = $span->status();
        self::assertNotNull($status);
        self::assertTrue($status->isError());
        self::assertSame('Connection refused', $status->description);

        $events = $span->events();
        self::assertCount(1, $events);
        self::assertSame('exception', $events[0]->name());
    }
}
