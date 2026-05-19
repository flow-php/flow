<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Transport\Receiver;

use Symfony\Component\Messenger\Envelope;

use function interface_exists;

if (interface_exists(KeepaliveReceiverInterface::class)) {
    return;
}

interface KeepaliveReceiverInterface extends ReceiverInterface
{
    public function keepalive(Envelope $envelope, ?int $seconds = null): void;
}
