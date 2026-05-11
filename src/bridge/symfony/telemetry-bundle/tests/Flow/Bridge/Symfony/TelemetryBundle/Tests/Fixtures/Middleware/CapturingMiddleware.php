<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Middleware;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;

final class CapturingMiddleware implements MiddlewareInterface
{
    public ?Envelope $captured = null;

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        $this->captured = $envelope;

        return $stack->next()->handle($envelope, $stack);
    }
}
