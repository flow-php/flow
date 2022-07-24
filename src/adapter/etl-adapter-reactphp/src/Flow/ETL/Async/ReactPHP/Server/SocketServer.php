<?php

declare(strict_types=1);

namespace Flow\ETL\Async\ReactPHP\Server;

use Flow\ETL\Async\Socket\Communication\MessageBuffer;
use Flow\ETL\Async\Socket\Server\Server;
use Flow\ETL\Async\Socket\Server\ServerProtocol;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\Serializer;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Socket\ConnectionInterface;
use React\Socket\ServerInterface;
use React\Socket\SocketServer as ReactSocketServer;

final class SocketServer implements Server
{
    /**
     * @var array<ConnectionInterface>
     */
    private array $connections;

    private ?ServerProtocol $handler;

    private ?LoopInterface $loop;

    private ?ServerInterface $server;

    private ?string $socketPath;

    private function __construct(
        private readonly string $host,
        private readonly LoggerInterface $logger,
        private readonly Serializer $serializer = new CompressingSerializer()
    ) {
        $this->loop = null;
        $this->server = null;
        $this->connections = [];
        $this->socketPath = null;
        $this->handler = null;
    }

    public static function tcp(
        int $port,
        LoggerInterface $logger,
        Serializer $serializer = new CompressingSerializer()
    ) : self {
        return new self("127.0.0.1:{$port}", $logger, $serializer);
    }

    public static function unixDomain(
        string $socketFolder,
        LoggerInterface $logger,
        Serializer $serializer = new CompressingSerializer()
    ) : self {
        $socketPath = \rtrim($socketFolder, '/');

        if (!\file_exists($socketPath) || !\is_dir($socketPath)) {
            throw new InvalidArgumentException("Given path does not exists or is not valid folder: {$socketPath}");
        }

        $socketPath = "{$socketPath}/" . \uniqid('socket') . '.sock';

        if (\strlen($socketPath) > 104) {
            throw new InvalidArgumentException('Given socket path is too long, max characters 104, given: ' . \strlen($socketPath));
        }

        $server = new self("unix://{$socketPath}", $logger, $serializer);
        $server->socketPath = $socketPath;

        return $server;
    }

    public function host() : string
    {
        return $this->host;
    }

    public function initialize(ServerProtocol $handler) : void
    {
        if ($this->server) {
            throw new RuntimeException('Server already initialized.');
        }

        $this->logger->debug('initializing server', [
            'host' => $this->host,
        ]);

        $this->loop = Loop::get();
        $this->server = new ReactSocketServer($this->host, [], $this->loop);

        $this->handler = $handler;
    }

    public function isRunning() : bool
    {
        return $this->server !== null;
    }

    public function start() : void
    {
        if ($this->loop === null) {
            throw new RuntimeException('Server not initialized.');
        }

        $this->logger->debug('starting server');

        $buffer = new MessageBuffer($this->serializer);

        $this->server->on('connection', function (ConnectionInterface $connection) use (&$buffer) : void {
            $this->connections[] = $connection;
            $this->logger->debug('client connected', ['address' => $connection->getRemoteAddress()]);

            $connection->on('data', function ($data) use ($connection, &$buffer) : void {
                $message = $buffer->buffer($connection, $data);

                if ($message !== null) {
                    $this->logger->debug('message received', [
                        'address' => $connection->getRemoteAddress(),
                        'message' => [
                            'type' => $message->type(),
                            'size' => \strlen($data),
                        ],
                    ]);

                    $this->handler->handle($message, new SocketClient($connection, $this->serializer), $this);
                }
            });

            $connection->on('error', function (\Throwable $e) : void {
                $this->logger->error('something went wrong', ['exception' => $e]);
            });

            $connection->on('close', function () use ($connection) : void {
                $this->logger->debug('client disconnected', [
                    'address' => $connection->getRemoteAddress(),
                ]);
            });
        });

        $this->loop->run();
    }

    public function stop() : void
    {
        if ($this->server === null) {
            throw new RuntimeException('Server already stopped.');
        }

        $this->logger->debug('stopping server', []);

        foreach ($this->connections as $connection) {
            $connection->close();
        }

        $this->connections = [];

        $this->server->close();
        $this->loop->stop();

        $this->loop = null;
        $this->server = null;
        $this->handler = null;

        if ($this->socketPath !== null && \file_exists($this->socketPath)) {
            \unlink($this->socketPath);
        }

        $this->logger->debug('server stopped', []);
    }
}
