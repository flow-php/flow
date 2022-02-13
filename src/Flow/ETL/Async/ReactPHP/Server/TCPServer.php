<?php

declare(strict_types=1);

namespace Flow\ETL\Async\ReactPHP\Server;

use Aeon\Calendar\Stopwatch;
use Flow\ETL\Async\Communication\Message;
use Flow\ETL\Async\Server\Server;
use Flow\ETL\Async\Server\ServerProtocol;
use Flow\ETL\Exception\RuntimeException;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\NativePHPSerializer;
use Flow\Serializer\Serializer;
use Psr\Log\LoggerInterface;
use React\EventLoop\LoopInterface;
use React\EventLoop\StreamSelectLoop;
use React\Socket\ConnectionInterface;
use React\Socket\ServerInterface;
use React\Socket\TcpServer as ReactTCPServer;

final class TCPServer implements Server
{
    private int $port;

    private LoggerInterface $logger;

    private ?LoopInterface $loop;

    private ?ServerInterface $server;

    private ?Serializer $serializer;

    private ?Stopwatch $stopwatch;

    /**
     * @var array<ConnectionInterface>
     */
    private array $connections;

    public function __construct(int $port, LoggerInterface $logger, ?Serializer $serializer = null)
    {
        $this->port = $port;
        $this->logger = $logger;
        $this->serializer = $serializer === null
            ? new CompressingSerializer(new NativePHPSerializer())
            : $serializer;
        $this->loop = null;
        $this->server = null;
        $this->stopwatch = null;
        $this->connections = [];
    }

    public function initialize(ServerProtocol $protocol) : void
    {
        if ($this->server) {
            throw new RuntimeException('Server already started.');
        }

        $this->logger->debug('[server] initializing server', [
            'port' => $this->port,
        ]);

        $this->loop = new StreamSelectLoop();
        $this->server = new ReactTCPServer("127.0.0.1:{$this->port}", $this->loop);

        $this->server->on('connection', function (ConnectionInterface $connection) use ($protocol) : void {
            $this->connections[] = $connection;
            $this->logger->debug('[server] client connected', ['address' => $connection->getRemoteAddress()]);

            $connection->on('data', function ($data) use ($connection, $protocol) : void {
                /** @var Message $message */
                $message = $this->serializer->unserialize($data);
                $this->logger->debug('[server] message received', [
                    'address' => $connection->getRemoteAddress(),
                    'message' => [
                        'type' => $message->type(),
                        'size' => \strlen($data),
                    ],
                ]);

                $protocol->handle($message, new TCPClient($connection, $this->serializer), $this);
            });

            $connection->on('error', function (\Throwable $e) : void {
                $this->logger->error('[server] something went wrong', ['exception' => $e]);
            });

            $connection->on('close', function () use ($connection) : void {
                $this->logger->debug('[server] client disconnected', [
                    'address' => $connection->getRemoteAddress(),
                ]);
            });
        });
    }

    public function start() : void
    {
        if ($this->loop === null) {
            throw new RuntimeException('Server already stopped.');
        }

        $this->stopwatch = new Stopwatch();
        $this->stopwatch->start();

        $this->logger->debug('[server] starting server');

        $this->loop->run();
    }

    public function stop() : void
    {
        if ($this->server === null) {
            throw new RuntimeException('Server already stopped.');
        }

        $this->stopwatch->stop();

        $this->logger->debug('[server] stopping server', [
            'total_time_sec' => $this->stopwatch->totalElapsedTime()->inSecondsPrecise(),
        ]);

        foreach ($this->connections as $connection) {
            $connection->close();
        }

        $this->connections = [];

        $this->server->close();
        $this->loop->stop();

        $this->stopwatch = null;
        $this->loop = null;
        $this->server = null;

        $this->logger->debug('[server] server stopped', []);
    }
}
