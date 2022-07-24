<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Amp\Server;

use function Amp\async;
use Amp\Socket;
use Amp\Socket\SocketServer as AmpSocketServer;
use Flow\ETL\Async\Socket\Communication\MessageBuffer;
use Flow\ETL\Async\Socket\Server\Server;
use Flow\ETL\Async\Socket\Server\ServerProtocol;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\Serializer;
use Psr\Log\LoggerInterface;

final class SocketServer implements Server
{
    private ?ServerProtocol $protocol;

    private ?AmpSocketServer $server;

    private ?string $socketPath;

    private function __construct(
        private readonly string $host,
        private readonly LoggerInterface $logger,
        private readonly Serializer $serializer = new CompressingSerializer()
    ) {
        $this->server = null;
        $this->protocol = null;
        $this->socketPath = null;
    }

    public static function tcp(
        int $port,
        LoggerInterface $logger,
        Serializer $serializer = new CompressingSerializer()
    ) : self {
        return new self("tcp://127.0.0.1:{$port}", $logger, $serializer);
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

        $this->server = Socket\listen($this->host);
        $this->protocol = $handler;
    }

    public function isRunning() : bool
    {
        return $this->server !== null;
    }

    public function start() : void
    {
        if ($this->server === null || $this->protocol === null) {
            throw new RuntimeException('Server not initialized.');
        }

        while ($socket = $this->server->accept()) {
            async(function () use ($socket) : void {
                $buffer = new MessageBuffer($this->serializer);

                while (null !== $data = $socket->read()) {
                    $message = $buffer->buffer($socket, $data);

                    if ($message !== null) {
                        $this->logger->debug('received from client', ['message' => ['type' => $message->type()]]);
                        /** @phpstan-ignore-next-line  */
                        $this->protocol->handle($message, new SocketClient($socket), $this);
                    }
                }
            });
        }
    }

    public function stop() : void
    {
        if ($this->server === null) {
            throw new RuntimeException('Server already stopped.');
        }

        $this->server->close();

        if ($this->socketPath !== null && \file_exists($this->socketPath)) {
            @\unlink($this->socketPath);
        }

        $this->server = null;
    }
}
