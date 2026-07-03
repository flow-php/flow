<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\CommandSuppressionSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Messenger\MessengerWorkerHarness;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;

use function interface_exists;

#[CoversClass(CommandSuppressionSubscriber::class)]
#[CoversClass(TracingMiddleware::class)]
final class MessengerTraceModeWiringTest extends KernelTestCase
{
    protected function setUp(): void
    {
        if (!interface_exists(MiddlewareInterface::class)) {
            self::markTestSkipped('symfony/messenger is not installed');
        }

        parent::setUp();
    }

    public function test_trace_true_suppresses_the_poll_but_records_the_handler(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'propagator' => ['type' => 'w3c'],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => ['enabled' => true, 'trace' => true],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.console.command_suppression_subscriber'));

        $subscriber = $container->get('flow.telemetry.console.command_suppression_subscriber');
        static::assertInstanceOf(EventSubscriberInterface::class, $subscriber);

        $names = MessengerWorkerHarness::drive($container, $subscriber);

        static::assertNotContains('poll.query', $names, 'the transport poll is suppressed');
        static::assertContains('process async', $names, 'the handler span is still recorded');
        static::assertContains('handler.work', $names, 'work inside the handler is still recorded');
    }

    public function test_trace_false_suppresses_the_whole_loop(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'propagator' => ['type' => 'w3c'],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => ['enabled' => true, 'trace' => false],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        $subscriber = $container->get('flow.telemetry.console.command_suppression_subscriber');
        static::assertInstanceOf(EventSubscriberInterface::class, $subscriber);

        $names = MessengerWorkerHarness::drive($container, $subscriber);

        static::assertNotContains('poll.query', $names);
        static::assertNotContains('process async', $names, 'no handler span when trace is disabled');
        static::assertNotContains('handler.work', $names, 'handler work is suppressed when trace is disabled');
    }
}
