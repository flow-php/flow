<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

final readonly class CurlTransportConfig
{
    /**
     * @param array<string, string> $headers
     */
    public function __construct(
        public string $endpoint,
        public array $headers,
        public int $timeoutMs,
        public int $connectTimeoutMs,
        public int $shutdownTimeoutMs,
        public bool $compression,
        public bool $followRedirects,
        public int $maxRedirects,
        public ?string $proxy,
        public bool $sslVerifyPeer,
        public bool $sslVerifyHost,
        public ?string $sslCertPath,
        public ?string $sslKeyPath,
        public ?string $caInfoPath,
        public SerializerType $serializer,
    ) {}
}
