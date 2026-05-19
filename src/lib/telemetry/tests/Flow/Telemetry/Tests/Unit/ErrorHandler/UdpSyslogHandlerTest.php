<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\ErrorHandler;

use Flow\Telemetry\ErrorHandler\UdpSyslogHandler;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function fclose;
use function is_resource;
use function stream_set_timeout;
use function stream_socket_get_name;
use function stream_socket_recvfrom;
use function stream_socket_server;
use function strrpos;
use function substr;

use const STREAM_SERVER_BIND;

final class UdpSyslogHandlerTest extends TestCase
{
    private int $port = 0;

    /** @var null|resource */
    private $receiver;

    protected function setUp(): void
    {
        $_errno = null;
        $errstr = null;
        $socket = stream_socket_server('udp://127.0.0.1:0', $_errno, $errstr, STREAM_SERVER_BIND);

        if ($socket === false) {
            self::markTestSkipped('Could not bind UDP socket: ' . ($errstr ?? 'unknown error'));
        }

        $this->receiver = $socket;
        $name = stream_socket_get_name($socket, false);

        if ($name === false) {
            self::markTestSkipped('Could not read UDP socket name');
        }

        $this->port = (int) substr($name, (int) strrpos($name, ':') + 1);
    }

    protected function tearDown(): void
    {
        if (is_resource($this->receiver)) {
            fclose($this->receiver);
            $this->receiver = null;
        }
    }

    public function test_sends_a_syslog_frame_over_udp(): void
    {
        static::assertIsResource($this->receiver);

        $handler = new UdpSyslogHandler('127.0.0.1', $this->port, ident: 'flow-test');

        $handler->handle(new RuntimeException('udp boom'));

        stream_set_timeout($this->receiver, 1);
        $datagram = stream_socket_recvfrom($this->receiver, 65535, 0);

        static::assertIsString($datagram);
        static::assertStringContainsString('flow-test', $datagram);
        static::assertStringContainsString('RuntimeException', $datagram);
        static::assertStringContainsString('udp boom', $datagram);
    }

    public function test_swallows_failures_after_receiver_closes(): void
    {
        static::assertIsResource($this->receiver);

        $handler = new UdpSyslogHandler('127.0.0.1', $this->port);

        $handler->handle(new RuntimeException('first'));

        fclose($this->receiver);
        $this->receiver = null;

        $handler->handle(new RuntimeException('second'));
    }

    public function test_throws_on_empty_host(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new UdpSyslogHandler('');
    }

    public function test_throws_on_empty_ident(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new UdpSyslogHandler('127.0.0.1', 514, ident: '');
    }

    public function test_throws_on_invalid_port(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new UdpSyslogHandler('127.0.0.1', 0);
    }
}
