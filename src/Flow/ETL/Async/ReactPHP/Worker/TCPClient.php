<?php

declare(strict_types=1);

namespace Flow\ETL\Async\ReactPHP\Worker;

use Aeon\Calendar\Stopwatch;
use Flow\ETL\Async\Client\Client;
use Flow\ETL\Async\Client\ClientProtocol;
use Flow\ETL\Async\Communication\Message;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\NativePHPSerializer;
use Flow\Serializer\Serializer;
use Psr\Log\LoggerInterface;
use React\EventLoop\StreamSelectLoop;
use React\Socket\ConnectionInterface;
use React\Socket\TcpConnector;

final class TCPClient implements Client
{
    private Serializer $serializer;

    private LoggerInterface $logger;

    public function __construct(LoggerInterface $logger, ?Serializer $serializer = null)
    {
        $this->serializer = $serializer === null
            ? new CompressingSerializer(new NativePHPSerializer())
            : $serializer;
        $this->logger = $logger;
    }

    public function connect(string $id, string $host, int $port, ClientProtocol $protocol) : void
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start();

        $this->logger->debug('[client] connecting to server', [
            'id' => $id,
            'host' => $host,
            'port' => $port,
        ]);

        $loop = new StreamSelectLoop();

        $connector = new TcpConnector($loop);

        $connector
            ->connect("{$host}:{$port}")
            ->then(
                function (ConnectionInterface $connection) use ($id, $protocol, $loop) : void {
                    $this->logger->debug('[client] connected to server ', [
                        'address' => $connection->getLocalAddress(),
                    ]);

                    $server = new TCPServer($connection, $this->serializer);

                    $connection->on('data', function ($data) use ($server, $protocol, $loop) : void {
                        /** @var Message $message */
                        $message = $this->serializer->unserialize($data);

                        $this->logger->debug('[client] received from server', [
                            'message' => [
                                'type' => $message->type(),
                                'size' => \strlen($data),
                            ],
                        ]);

                        $protocol->handle($message, $server);
                    });

                    $connection->on('error', function (\Throwable $e) : void {
                        $this->logger->error('[client] something went wrong', ['exception' => $e]);
                    });

                    $connection->on('close', function () use ($loop) : void {
                        $this->logger->debug('[client] server closes connection', []);
                        $loop->stop();
                    });

                    $protocol->identify($id, $server);
                },
                function (\Throwable $error) : void {
                    $this->logger->error('[client] connection closed due to internal error', [
                        'exception' => $error,
                    ]);
                }
            );

        $loop->run();

        $stopwatch->stop();

        $this->logger->debug(
            '[client] client stopped',
            [
                'total_connection_time_sec' => $stopwatch->totalElapsedTime()->inSecondsPrecise(), ]
        );
    }
}
