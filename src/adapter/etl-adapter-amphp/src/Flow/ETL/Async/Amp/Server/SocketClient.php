<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Amp\Server;

use Amp\Socket\EncryptableSocket;
use Flow\ETL\Async\Socket\Communication\Message;
use Flow\ETL\Async\Socket\Server\Client;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\Serializer;

final class SocketClient implements Client
{
    public function __construct(
        private readonly EncryptableSocket $socket,
        private readonly Serializer $serializer = new CompressingSerializer()
    ) {
    }

    public function disconnect() : void
    {
        $this->socket->close();
    }

    public function send(Message $message) : void
    {
        $this->socket->write($message->toString($this->serializer));
    }
}
