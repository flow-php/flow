<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Amp\Worker;

use Amp\Socket\Socket;
use Flow\ETL\Async\Socket\Communication\Message;
use Flow\ETL\Async\Socket\Worker\Server;
use Flow\Serializer\Serializer;

final class SocketServer implements Server
{
    private Serializer $serializer;

    private Socket $socket;

    public function __construct(Socket $socket, Serializer $serializer)
    {
        $this->socket = $socket;
        $this->serializer = $serializer;
    }

    public function send(Message $message) : void
    {
        $this->socket->write($message->toString($this->serializer));
    }
}
