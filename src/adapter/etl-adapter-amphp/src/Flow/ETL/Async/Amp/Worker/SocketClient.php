<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Amp\Worker;

use function Amp\Socket\connect;
use Amp\Socket\ConnectContext;
use Flow\ETL\Async\Socket\Communication\MessageBuffer;
use Flow\ETL\Async\Socket\Worker\Client;
use Flow\ETL\Async\Socket\Worker\ClientProtocol;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\Serializer;
use Psr\Log\LoggerInterface;

final class SocketClient implements Client
{
    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly Serializer $serializer = new CompressingSerializer()
    ) {
    }

    public function connect(string $id, string $host, ClientProtocol $protocol) : void
    {
        $socket = connect($host, new ConnectContext());

        $server = new SocketServer($socket, $this->serializer);

        $protocol->identify($id, $server);

        $buffer = new MessageBuffer($this->serializer);

        while (null !== $data = $socket->read()) {
            $message = $buffer->buffer($socket, $data);

            if ($message !== null) {
                $this->logger->debug('received from server', ['message' => ['type' => $message->type()]]);
                $protocol->handle($id, $message, $server);
            }
        }

        $this->logger->debug('connection closed by server');
    }
}
