<?php

declare(strict_types=1);

namespace Flow\ETL\Async\ReactPHP\Server;

use Flow\ETL\Async\Communication\Message;
use Flow\ETL\Async\Server\Client;
use Flow\Serializer\Serializer;
use React\Socket\ConnectionInterface;

final class TCPClient implements Client
{
    private ConnectionInterface $connection;

    private Serializer $serializer;

    public function __construct(ConnectionInterface $connection, Serializer $serializer)
    {
        $this->connection = $connection;
        $this->serializer = $serializer;
    }

    public function send(Message $message) : void
    {
        $this->connection->write($this->serializer->serialize($message));
    }

    public function disconnect() : void
    {
        $this->connection->close();
    }
}
