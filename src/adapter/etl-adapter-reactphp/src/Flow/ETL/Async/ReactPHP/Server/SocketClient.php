<?php

declare(strict_types=1);

namespace Flow\ETL\Async\ReactPHP\Server;

use Flow\ETL\Async\Socket\Communication\Message;
use Flow\ETL\Async\Socket\Server\Client;
use Flow\Serializer\Serializer;
use React\Socket\ConnectionInterface;

final class SocketClient implements Client
{
    public function __construct(private readonly ConnectionInterface $connection, private readonly Serializer $serializer)
    {
    }

    public function disconnect() : void
    {
        $this->connection->close();
    }

    public function send(Message $message) : void
    {
        $serializedMessage = $this->serializer->serialize($message);
        $packet = '|' . $serializedMessage . '|';

        $this->connection->write($packet);
    }
}
