<?php

declare(strict_types=1);

namespace Flow\ETL\Async\ReactPHP\Worker;

use Flow\ETL\Async\Socket\Communication\MessageBuffer;
use Flow\ETL\Async\Socket\Worker\Client;
use Flow\ETL\Async\Socket\Worker\ClientProtocol;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\Serializer;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\Socket\ConnectionInterface;
use React\Socket\Connector;

final class SocketClient implements Client
{
    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly Serializer $serializer = new CompressingSerializer()
    ) {
    }

    public function connect(string $id, string $host, ClientProtocol $protocol) : void
    {
        $this->logger->debug('connecting to server', [
            'id' => $id,
            'host' => $host,
        ]);

        $loop = Loop::get();

        $connector = new Connector([], $loop);

        $buffer = new MessageBuffer($this->serializer);

        $connector
            ->connect("{$host}")
            ->then(
                function (ConnectionInterface $connection) use ($id, $protocol, $loop, &$buffer) : void {
                    $this->logger->debug('connected to server ', [
                        'address' => $connection->getLocalAddress(),
                    ]);

                    $server = new SocketServer($connection, $this->serializer);

                    $connection->on('data', function ($data) use ($id, $server, $connection, $protocol, &$buffer) : void {
                        $message = $buffer->buffer($connection, $data);

                        if ($message !== null) {
                            $this->logger->debug('received from server', ['message' => ['type' => $message->type()]]);
                            $protocol->handle($id, $message, $server);
                        }
                    });

                    $connection->on('error', function (\Throwable $e) : void {
                        $this->logger->error('something went wrong', ['exception' => $e, 'trace' => $e->getTraceAsString()]);
                    });

                    $connection->on('close', function () use ($loop) : void {
                        $this->logger->debug('server closes connection', []);
                        $loop->stop();
                    });

                    $protocol->identify($id, $server);
                },
                function (\Throwable $error) : void {
                    $this->logger->error('connection closed due to internal error', [
                        'exception' => $error,
                    ]);
                }
            );

        $loop->run();

        $this->logger->debug('client stopped');
    }
}
