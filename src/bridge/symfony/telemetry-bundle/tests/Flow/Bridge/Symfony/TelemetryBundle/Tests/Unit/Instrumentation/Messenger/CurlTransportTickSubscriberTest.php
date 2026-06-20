<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\CurlTransportTickSubscriber;
use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Bridge\Telemetry\OTLP\Transport\CurlTransport;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Worker;

use function extension_loaded;

#[CoversClass(CurlTransportTickSubscriber::class)]
final class CurlTransportTickSubscriberTest extends TestCase
{
    public function test_subscribed_events(): void
    {
        static::assertSame(
            [WorkerRunningEvent::class => 'onWorkerRunning'],
            CurlTransportTickSubscriber::getSubscribedEvents(),
        );
    }

    public function test_on_worker_running_ticks_every_transport(): void
    {
        if (!extension_loaded('curl')) {
            static::markTestSkipped('ext-curl is required');
        }

        $transports = [
            new CurlTransport('http://localhost:4318', new JsonSerializer()),
            new CurlTransport('http://localhost:4318', new JsonSerializer()),
        ];

        $bus = new class implements MessageBusInterface {
            public function dispatch(object $message, array $stamps = []): Envelope
            {
                return new Envelope($message);
            }
        };

        (new CurlTransportTickSubscriber($transports))->onWorkerRunning(
            new WorkerRunningEvent(new Worker([], $bus), false),
        );

        $this->addToAssertionCount(1);

        foreach ($transports as $transport) {
            $transport->shutdown();
        }
    }
}
