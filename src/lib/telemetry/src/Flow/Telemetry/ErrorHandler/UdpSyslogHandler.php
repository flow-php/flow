<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

/**
 * Writes RFC 5424-style syslog frames over UDP to a remote collector.
 *
 * The socket is opened lazily on the first handle() call, persisted across
 * subsequent calls, and closed in __destruct().
 */
final class UdpSyslogHandler implements ErrorHandler
{
    /** @var null|resource */
    private $socket;

    public function __construct(
        private readonly string $host,
        private readonly int $port = 514,
        private readonly string $ident = 'flow-telemetry',
        private readonly SyslogFacility $facility = SyslogFacility::User,
        private readonly SyslogSeverity $severity = SyslogSeverity::Error,
    ) {
        if ($host === '') {
            throw new \InvalidArgumentException('UdpSyslogHandler host must be a non-empty string');
        }

        if ($port < 1 || $port > 65535) {
            throw new \InvalidArgumentException('UdpSyslogHandler port must be between 1 and 65535');
        }

        if ($ident === '') {
            throw new \InvalidArgumentException('UdpSyslogHandler ident must be a non-empty string');
        }
    }

    public function __destruct()
    {
        if (\is_resource($this->socket)) {
            @\fclose($this->socket);
            $this->socket = null;
        }
    }

    public function handle(\Throwable $error): void
    {
        try {
            $socket = $this->openSocket();

            if ($socket === null) {
                return;
            }

            $priority = $this->facility->value | $this->severity->value;
            $timestamp = (new \DateTimeImmutable())->format(\DateTimeInterface::RFC3339);
            $hostname = \gethostname();

            if ($hostname === false) {
                $hostname = '-';
            }

            $message = \sprintf(
                '<%d>1 %s %s %s - - - %s: %s in %s:%d',
                $priority,
                $timestamp,
                $hostname,
                $this->ident,
                $error::class,
                $error->getMessage(),
                $error->getFile(),
                $error->getLine(),
            );

            @\fwrite($socket, $message);
        } catch (\Throwable) {
        }
    }

    /**
     * @return null|resource
     */
    private function openSocket()
    {
        if (\is_resource($this->socket)) {
            return $this->socket;
        }

        $handle = @\stream_socket_client(\sprintf('udp://%s:%d', $this->host, $this->port), $_errno, $_errstr, 1.0);

        if (!\is_resource($handle)) {
            return null;
        }

        $this->socket = $handle;

        return $this->socket;
    }
}
