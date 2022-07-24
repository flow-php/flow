<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Amp\Worker;

use Amp\Socket\EncryptableSocket;
use Flow\ETL\Async\Socket\Communication\Message;
use Flow\ETL\Async\Socket\Worker\Server;
use Flow\Serializer\Serializer;

final class SocketServer implements Server
{
    private Serializer $serializer;

    private EncryptableSocket $socket;

    public function __construct(EncryptableSocket $socket, Serializer $serializer)
    {
        $this->socket = $socket;
        $this->serializer = $serializer;
    }

    public function send(Message $message) : void
    {
        $this->socket->write($message->toString($this->serializer));
    }
}
