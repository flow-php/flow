<?php

declare(strict_types=1);

namespace Flow\ETL\Async\ReactPHP\Worker;

use Flow\ETL\Async\Socket\Communication\Message;
use Flow\ETL\Async\Socket\Worker\Server;
use Flow\Serializer\Serializer;
use React\Socket\ConnectionInterface;

final class SocketServer implements Server
{
    public function __construct(private readonly ConnectionInterface $connection, private readonly Serializer $serializer)
    {
    }

    public function send(Message $message) : void
    {
        $this->connection->write($message->toString($this->serializer));
    }
}
