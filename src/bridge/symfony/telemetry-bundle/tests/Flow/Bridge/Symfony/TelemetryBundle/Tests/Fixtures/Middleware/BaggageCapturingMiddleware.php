<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Middleware;

use Flow\Telemetry\Context\Baggage;
use Flow\Telemetry\Context\ContextStorage;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;

final class BaggageCapturingMiddleware implements MiddlewareInterface
{
    public ?Baggage $capturedDuringHandling = null;

    public function __construct(
        private readonly ContextStorage $contextStorage,
    ) {}

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        $this->capturedDuringHandling = $this->contextStorage->current()->baggage;

        return $stack->next()->handle($envelope, $stack);
    }
}
