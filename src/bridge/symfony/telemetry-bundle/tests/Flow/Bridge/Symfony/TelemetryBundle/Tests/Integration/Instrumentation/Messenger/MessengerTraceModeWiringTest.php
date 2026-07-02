<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\ConsumeCommandSuppressionSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\WorkerReceiveCycleSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Messenger\MessengerWorkerHarness;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;

use function interface_exists;

#[CoversClass(WorkerReceiveCycleSubscriber::class)]
#[CoversClass(ConsumeCommandSuppressionSubscriber::class)]
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

    public function test_trace_worker_emits_the_receive_span_and_records_the_poll(): void
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
                        'messenger' => ['enabled' => true, 'trace' => 'worker'],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.messenger.worker_receive_cycle_subscriber'));
        static::assertFalse($container->has('flow.telemetry.messenger.consume_command_suppression_subscriber'));

        $subscriber = $container->get('flow.telemetry.messenger.worker_receive_cycle_subscriber');
        static::assertInstanceOf(EventSubscriberInterface::class, $subscriber);

        $names = MessengerWorkerHarness::drive($container, $subscriber);

        static::assertContains('messenger.receive', $names);
        static::assertContains('poll.query', $names, 'the transport poll is recorded under the cycle span');
    }

    public function test_trace_handlers_suppresses_the_poll_but_records_the_handler(): void
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
                        'messenger' => ['enabled' => true, 'trace' => 'handlers', 'link' => 'dispatcher'],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.messenger.consume_command_suppression_subscriber'));
        static::assertFalse($container->has('flow.telemetry.messenger.worker_receive_cycle_subscriber'));

        $subscriber = $container->get('flow.telemetry.messenger.consume_command_suppression_subscriber');
        static::assertInstanceOf(EventSubscriberInterface::class, $subscriber);

        $names = MessengerWorkerHarness::drive($container, $subscriber);

        static::assertNotContains('messenger.receive', $names, 'no cycle span when the worker is not traced');
        static::assertNotContains('poll.query', $names, 'the transport poll is suppressed');
        static::assertContains('process TestMessage', $names, 'the handler span is still recorded');
        static::assertContains('handler.work', $names, 'work inside the handler is still recorded');
    }

    public function test_trace_none_suppresses_the_whole_loop(): void
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
                        'messenger' => ['enabled' => true, 'trace' => 'none', 'link' => 'dispatcher'],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.messenger.consume_command_suppression_subscriber'));

        $subscriber = $container->get('flow.telemetry.messenger.consume_command_suppression_subscriber');
        static::assertInstanceOf(EventSubscriberInterface::class, $subscriber);

        $names = MessengerWorkerHarness::drive($container, $subscriber);

        static::assertNotContains('messenger.receive', $names);
        static::assertNotContains('poll.query', $names);
        static::assertNotContains('process TestMessage', $names, 'no handler span in metrics-only mode');
        static::assertNotContains('handler.work', $names, 'handler work is suppressed in metrics-only mode');
    }
}
