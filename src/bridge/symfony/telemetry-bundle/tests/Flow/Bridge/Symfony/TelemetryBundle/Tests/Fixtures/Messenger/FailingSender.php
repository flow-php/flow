<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Messenger;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;

final class FailingSender implements SenderInterface
{
    public function send(Envelope $envelope): Envelope
    {
        throw new TransportException('connection refused');
    }
}
