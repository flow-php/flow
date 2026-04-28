<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr18\Telemetry\Tests\Integration;

/**
 * Forks a child process that serves a fixed 200 OK on a free loopback port
 * for one test. Avoids depending on public URLs or PHP's built-in web server
 * (the latter writes a lock file that's not always writable under sandboxes).
 */
final class LocalHttpServer
{
    private int $childPid = 0;

    private int $port = 0;

    public function port() : int
    {
        return $this->port;
    }

    public function start() : void
    {
        if (!\function_exists('pcntl_fork')) {
            throw new \RuntimeException('pcntl_fork() is required to run the local test HTTP server.');
        }

        $server = \stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);

        if (!\is_resource($server)) {
            throw new \RuntimeException(\sprintf('Failed to bind local test HTTP server: %s (%d)', $errstr, $errno));
        }

        $name = \stream_socket_get_name($server, false);

        if (!\is_string($name)) {
            \fclose($server);

            throw new \RuntimeException('Failed to read bound socket name.');
        }

        $colon = \strrpos($name, ':');

        if ($colon === false) {
            \fclose($server);

            throw new \RuntimeException(\sprintf('Unexpected socket name "%s".', $name));
        }

        $this->port = (int) \substr($name, $colon + 1);

        $pid = \pcntl_fork();

        if ($pid === -1) {
            \fclose($server);

            throw new \RuntimeException('pcntl_fork() failed.');
        }

        if ($pid === 0) {
            $this->serveLoop($server);
        }

        \fclose($server);
        $this->childPid = $pid;
    }

    public function stop() : void
    {
        if ($this->childPid > 0) {
            @\posix_kill($this->childPid, \SIGTERM);
            \pcntl_waitpid($this->childPid, $status);
            $this->childPid = 0;
        }
    }

    public function url() : string
    {
        return \sprintf('http://127.0.0.1:%d', $this->port);
    }

    /**
     * @param resource $server
     */
    private function serveLoop($server) : never
    {
        \pcntl_signal(\SIGTERM, static function () : void {
            exit(0);
        });

        while (true) {
            \pcntl_signal_dispatch();
            $client = @\stream_socket_accept($server, 1.0);

            if (!\is_resource($client)) {
                continue;
            }

            \stream_set_timeout($client, 1);

            // Read request line + headers; we don't actually need the body.
            while (!\feof($client)) {
                $line = \fgets($client, 8192);

                if ($line === false || $line === "\r\n" || $line === "\n") {
                    break;
                }
            }

            $body = 'ok';
            $response = "HTTP/1.1 200 OK\r\n"
                . "Content-Type: text/plain\r\n"
                . 'Content-Length: ' . \strlen($body) . "\r\n"
                . "Connection: close\r\n"
                . "\r\n"
                . $body;

            \fwrite($client, $response);
            \fclose($client);
        }
    }
}
