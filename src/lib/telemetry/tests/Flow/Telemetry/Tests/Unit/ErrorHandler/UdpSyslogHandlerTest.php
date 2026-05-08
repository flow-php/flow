<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\ErrorHandler;

use Flow\Telemetry\ErrorHandler\UdpSyslogHandler;
use PHPUnit\Framework\TestCase;

final class UdpSyslogHandlerTest extends TestCase
{
    private int $port = 0;

    /** @var null|resource */
    private $receiver;

    protected function setUp() : void
    {
        $socket = \stream_socket_server('udp://127.0.0.1:0', $errno, $errstr, \STREAM_SERVER_BIND);

        if ($socket === false) {
            self::markTestSkipped('Could not bind UDP socket: ' . $errstr);
        }

        $this->receiver = $socket;
        $name = \stream_socket_get_name($socket, false);

        if ($name === false) {
            self::markTestSkipped('Could not read UDP socket name');
        }

        $this->port = (int) \substr($name, (int) \strrpos($name, ':') + 1);
    }

    protected function tearDown() : void
    {
        if (\is_resource($this->receiver)) {
            \fclose($this->receiver);
            $this->receiver = null;
        }
    }

    public function test_sends_a_syslog_frame_over_udp() : void
    {
        self::assertIsResource($this->receiver);

        $handler = new UdpSyslogHandler('127.0.0.1', $this->port, ident: 'flow-test');

        $handler->handle(new \RuntimeException('udp boom'));

        \stream_set_timeout($this->receiver, 1);
        $datagram = \stream_socket_recvfrom($this->receiver, 65535);

        self::assertIsString($datagram);
        self::assertStringContainsString('flow-test', $datagram);
        self::assertStringContainsString('RuntimeException', $datagram);
        self::assertStringContainsString('udp boom', $datagram);
    }

    public function test_swallows_failures_after_receiver_closes() : void
    {
        self::assertIsResource($this->receiver);

        $handler = new UdpSyslogHandler('127.0.0.1', $this->port);

        $handler->handle(new \RuntimeException('first'));

        \fclose($this->receiver);
        $this->receiver = null;

        $handler->handle(new \RuntimeException('second'));
    }

    public function test_throws_on_empty_host() : void
    {
        $this->expectException(\InvalidArgumentException::class);

        new UdpSyslogHandler('');
    }

    public function test_throws_on_empty_ident() : void
    {
        $this->expectException(\InvalidArgumentException::class);

        new UdpSyslogHandler('127.0.0.1', 514, ident: '');
    }

    public function test_throws_on_invalid_port() : void
    {
        $this->expectException(\InvalidArgumentException::class);

        new UdpSyslogHandler('127.0.0.1', 0);
    }
}
