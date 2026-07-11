<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\AsyncCurlTransportTickSubscriber;
use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Bridge\Telemetry\OTLP\Transport\AsyncCurlTransport;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Worker;

use function extension_loaded;

#[CoversClass(AsyncCurlTransportTickSubscriber::class)]
final class AsyncCurlTransportTickSubscriberTest extends TestCase
{
    public function test_subscribed_events(): void
    {
        static::assertSame(
            [WorkerRunningEvent::class => 'onWorkerRunning'],
            AsyncCurlTransportTickSubscriber::getSubscribedEvents(),
        );
    }

    public function test_on_worker_running_ticks_every_transport(): void
    {
        if (!extension_loaded('curl')) {
            static::markTestSkipped('ext-curl is required');
        }

        $transports = [
            new AsyncCurlTransport('http://localhost:4318', new JsonSerializer()),
            new AsyncCurlTransport('http://localhost:4318', new JsonSerializer()),
        ];

        $bus = new class implements MessageBusInterface {
            public function dispatch(object $message, array $stamps = []): Envelope
            {
                return new Envelope($message);
            }
        };

        (new AsyncCurlTransportTickSubscriber($transports))->onWorkerRunning(
            new WorkerRunningEvent(new Worker([], $bus), false),
        );

        $this->addToAssertionCount(1);

        foreach ($transports as $transport) {
            $transport->shutdown();
        }
    }
}
