<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use InvalidArgumentException;

use const CURLOPT_CAINFO;
use const CURLOPT_CONNECTTIMEOUT_MS;
use const CURLOPT_ENCODING;
use const CURLOPT_FOLLOWLOCATION;
use const CURLOPT_HTTPHEADER;
use const CURLOPT_MAXREDIRS;
use const CURLOPT_POST;
use const CURLOPT_POSTFIELDS;
use const CURLOPT_PROXY;
use const CURLOPT_RETURNTRANSFER;
use const CURLOPT_SSL_VERIFYHOST;
use const CURLOPT_SSL_VERIFYPEER;
use const CURLOPT_SSLCERT;
use const CURLOPT_SSLKEY;
use const CURLOPT_TIMEOUT_MS;
use const CURLOPT_URL;

/**
 * Configuration options for CurlTransport.
 *
 * Provides a fluent interface for configuring curl transport options.
 *
 * Example usage:
 * ```php
 * $options = otlp_curl_options()
 *     ->withTimeout(2000)
 *     ->withConnectTimeout(500)
 *     ->withHeader('Authorization', 'Bearer token')
 *     ->withCompression()
 *     ->withSslVerification(verifyPeer: true);
 *
 * $transport = otlp_curl_transport($endpoint, $serializer, $options);
 * ```
 */
final class CurlTransportOptions
{
    public const int DEFAULT_CONNECT_TIMEOUT_MS = 250;

    public const int DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000;

    public const int DEFAULT_TIMEOUT_MS = 10000;

    private ?string $caInfoPath = null;

    private bool $compression = false;

    private int $connectTimeoutMs = self::DEFAULT_CONNECT_TIMEOUT_MS;

    private bool $followRedirects = true;

    /** @var array<string, string> */
    private array $headers = [];

    private int $maxRedirects = 3;

    private ?string $proxy = null;

    private int $shutdownTimeoutMs = self::DEFAULT_SHUTDOWN_TIMEOUT_MS;

    private ?string $sslCertPath = null;

    private ?string $sslKeyPath = null;

    private bool $sslVerifyHost = true;

    private bool $sslVerifyPeer = true;

    private int $timeoutMs = self::DEFAULT_TIMEOUT_MS;

    public function caInfoPath(): ?string
    {
        return $this->caInfoPath;
    }

    public function compression(): bool
    {
        return $this->compression;
    }

    public function connectTimeoutMs(): int
    {
        return $this->connectTimeoutMs;
    }

    public function followRedirects(): bool
    {
        return $this->followRedirects;
    }

    /**
     * @return array<string, string>
     */
    public function headers(): array
    {
        return $this->headers;
    }

    public function maxRedirects(): int
    {
        return $this->maxRedirects;
    }

    public function proxy(): ?string
    {
        return $this->proxy;
    }

    public function shutdownTimeoutMs(): int
    {
        return $this->shutdownTimeoutMs;
    }

    public function sslCertPath(): ?string
    {
        return $this->sslCertPath;
    }

    public function sslKeyPath(): ?string
    {
        return $this->sslKeyPath;
    }

    public function sslVerifyHost(): bool
    {
        return $this->sslVerifyHost;
    }

    public function sslVerifyPeer(): bool
    {
        return $this->sslVerifyPeer;
    }

    public function timeoutMs(): int
    {
        return $this->timeoutMs;
    }

    /**
     * Build curl options array for a request.
     *
     * @param string $url Request URL
     * @param string $body Request body
     * @param list<string> $headers HTTP headers in "Name: value" format
     *
     * @return array<int, mixed> Curl options array for curl_setopt_array()
     */
    public function toCurlOptions(string $url, string $body, array $headers): array
    {
        $curlOptions = [
            CURLOPT_URL => $url,
            CURLOPT_POST => true,
            CURLOPT_POSTFIELDS => $body,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT_MS => $this->timeoutMs,
            CURLOPT_CONNECTTIMEOUT_MS => $this->connectTimeoutMs,
            CURLOPT_HTTPHEADER => $headers,
            CURLOPT_FOLLOWLOCATION => $this->followRedirects,
            CURLOPT_MAXREDIRS => $this->maxRedirects,
            CURLOPT_SSL_VERIFYPEER => $this->sslVerifyPeer,
            CURLOPT_SSL_VERIFYHOST => $this->sslVerifyHost ? 2 : 0,
        ];

        if ($this->sslCertPath !== null) {
            $curlOptions[CURLOPT_SSLCERT] = $this->sslCertPath;
        }

        if ($this->sslKeyPath !== null) {
            $curlOptions[CURLOPT_SSLKEY] = $this->sslKeyPath;
        }

        if ($this->caInfoPath !== null) {
            $curlOptions[CURLOPT_CAINFO] = $this->caInfoPath;
        }

        if ($this->proxy !== null) {
            $curlOptions[CURLOPT_PROXY] = $this->proxy;
        }

        if ($this->compression) {
            $curlOptions[CURLOPT_ENCODING] = '';
        }

        return $curlOptions;
    }

    /**
     * Set the path to CA certificate bundle.
     *
     * @param string $caInfoPath Path to CA certificate bundle file
     */
    public function withCaInfo(string $caInfoPath): self
    {
        $this->caInfoPath = $caInfoPath;

        return $this;
    }

    /**
     * Enable or disable automatic decompression of responses.
     *
     * When enabled, curl will send Accept-Encoding header and automatically
     * decompress gzip, deflate, or br compressed responses.
     *
     * @param bool $enabled Whether to enable compression (default: true)
     */
    public function withCompression(bool $enabled = true): self
    {
        $this->compression = $enabled;

        return $this;
    }

    /**
     * Set the connection timeout in milliseconds.
     *
     * @param int $milliseconds Maximum time in milliseconds to wait for the TCP/TLS connection
     */
    public function withConnectTimeout(int $milliseconds): self
    {
        if ($milliseconds < 0) {
            throw new InvalidArgumentException('Connect timeout must be non-negative');
        }

        $this->connectTimeoutMs = $milliseconds;

        return $this;
    }

    /**
     * Configure redirect following behavior.
     *
     * @param bool $follow Whether to follow redirects
     * @param int $maxRedirects Maximum number of redirects to follow (default: 3)
     */
    public function withFollowRedirects(bool $follow, int $maxRedirects = 3): self
    {
        if ($maxRedirects < 0) {
            throw new InvalidArgumentException('Max redirects must be non-negative');
        }

        $this->followRedirects = $follow;
        $this->maxRedirects = $maxRedirects;

        return $this;
    }

    /**
     * Add a single header to the request.
     *
     * @param string $name Header name
     * @param string $value Header value
     */
    public function withHeader(string $name, string $value): self
    {
        $this->headers[$name] = $value;

        return $this;
    }

    /**
     * Set multiple headers at once.
     *
     * This replaces any previously set headers.
     *
     * @param array<string, string> $headers Headers as name => value pairs
     */
    public function withHeaders(array $headers): self
    {
        $this->headers = $headers;

        return $this;
    }

    /**
     * Set a proxy server.
     *
     * @param string $proxy Proxy URL (e.g., 'http://proxy:8080', 'socks5://proxy:1080')
     */
    public function withProxy(string $proxy): self
    {
        $this->proxy = $proxy;

        return $this;
    }

    /**
     * Set the wall-clock budget for draining pending requests at shutdown.
     *
     * Requests still pending after this deadline are abandoned and reported as failed
     * (forwarded to a configured failover transport, otherwise aggregated into the
     * shutdown TransportException). Steady-state flush() is unaffected by this knob.
     *
     * @param int $milliseconds Maximum drain wall-clock at shutdown
     */
    public function withShutdownTimeout(int $milliseconds): self
    {
        if ($milliseconds < 0) {
            throw new InvalidArgumentException('Shutdown timeout must be non-negative');
        }

        $this->shutdownTimeoutMs = $milliseconds;

        return $this;
    }

    /**
     * Set SSL client certificate.
     *
     * @param string $certPath Path to client certificate file (PEM format)
     * @param null|string $keyPath Optional path to private key file (if not included in cert)
     */
    public function withSslCertificate(string $certPath, ?string $keyPath = null): self
    {
        $this->sslCertPath = $certPath;
        $this->sslKeyPath = $keyPath;

        return $this;
    }

    /**
     * Configure SSL/TLS verification.
     *
     * @param bool $verifyPeer Whether to verify the peer's SSL certificate
     * @param bool $verifyHost Whether to verify the certificate's name against the host (default: true)
     */
    public function withSslVerification(bool $verifyPeer, bool $verifyHost = true): self
    {
        $this->sslVerifyPeer = $verifyPeer;
        $this->sslVerifyHost = $verifyHost;

        return $this;
    }

    /**
     * Set the request timeout in milliseconds.
     *
     * @param int $milliseconds Maximum time in milliseconds for the entire request (connect + send + receive)
     */
    public function withTimeout(int $milliseconds): self
    {
        if ($milliseconds < 0) {
            throw new InvalidArgumentException('Timeout must be non-negative');
        }

        $this->timeoutMs = $milliseconds;

        return $this;
    }
}
