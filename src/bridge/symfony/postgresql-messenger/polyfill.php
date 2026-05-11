<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Transport\Receiver;

if (\interface_exists(KeepaliveReceiverInterface::class)) {
    return;
}

interface KeepaliveReceiverInterface extends ReceiverInterface
{
    public function keepalive(\Symfony\Component\Messenger\Envelope $envelope, ?int $seconds = null): void;
}
