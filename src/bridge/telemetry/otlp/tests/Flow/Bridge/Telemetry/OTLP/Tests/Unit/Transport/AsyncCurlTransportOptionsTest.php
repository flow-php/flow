<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Transport;

use Flow\Bridge\Telemetry\OTLP\Transport\AsyncCurlTransportOptions;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_async_curl_options;

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

final class AsyncCurlTransportOptionsTest extends TestCase
{
    public function test_default_options(): void
    {
        $options = new AsyncCurlTransportOptions();

        static::assertSame(AsyncCurlTransportOptions::DEFAULT_TIMEOUT_MS, $options->timeoutMs());
        static::assertSame(AsyncCurlTransportOptions::DEFAULT_CONNECT_TIMEOUT_MS, $options->connectTimeoutMs());
        static::assertSame([], $options->headers());
        static::assertTrue($options->followRedirects());
        static::assertSame(3, $options->maxRedirects());
        static::assertTrue($options->sslVerifyPeer());
        static::assertTrue($options->sslVerifyHost());
        static::assertNull($options->sslCertPath());
        static::assertNull($options->sslKeyPath());
        static::assertNull($options->caInfoPath());
        static::assertNull($options->proxy());
        static::assertFalse($options->compression());
    }

    public function test_default_pump_timeout(): void
    {
        $options = new AsyncCurlTransportOptions();

        static::assertSame(AsyncCurlTransportOptions::DEFAULT_PUMP_TIMEOUT_MS, $options->pumpTimeoutMs());
    }

    public function test_default_shutdown_timeout(): void
    {
        $options = new AsyncCurlTransportOptions();

        static::assertSame(AsyncCurlTransportOptions::DEFAULT_SHUTDOWN_TIMEOUT_MS, $options->shutdownTimeoutMs());
    }

    public function test_with_pump_timeout(): void
    {
        $options = (new AsyncCurlTransportOptions())->withPumpTimeout(250);

        static::assertSame(250, $options->pumpTimeoutMs());
    }

    public function test_with_pump_timeout_accepts_zero(): void
    {
        $options = (new AsyncCurlTransportOptions())->withPumpTimeout(0);

        static::assertSame(0, $options->pumpTimeoutMs());
    }

    public function test_with_pump_timeout_rejects_negative_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pump timeout must be non-negative');

        (new AsyncCurlTransportOptions())->withPumpTimeout(-1);
    }

    public function test_dsl_function_creates_options(): void
    {
        $options = otlp_async_curl_options();

        static::assertInstanceOf(AsyncCurlTransportOptions::class, $options);
        static::assertSame(AsyncCurlTransportOptions::DEFAULT_TIMEOUT_MS, $options->timeoutMs());
    }

    public function test_fluent_chaining(): void
    {
        $options = otlp_async_curl_options()
            ->withTimeout(2000)
            ->withConnectTimeout(500)
            ->withHeader('Authorization', 'Bearer token')
            ->withFollowRedirects(true, 5)
            ->withSslVerification(true)
            ->withProxy('http://proxy:8080')
            ->withCompression();

        static::assertSame(2000, $options->timeoutMs());
        static::assertSame(500, $options->connectTimeoutMs());
        static::assertSame(['Authorization' => 'Bearer token'], $options->headers());
        static::assertTrue($options->followRedirects());
        static::assertSame(5, $options->maxRedirects());
        static::assertTrue($options->sslVerifyPeer());
        static::assertSame('http://proxy:8080', $options->proxy());
        static::assertTrue($options->compression());
    }

    public function test_to_curl_options_basic(): void
    {
        $options = new AsyncCurlTransportOptions();

        $curlOptions = $options->toCurlOptions(
            'http://localhost:4318/v1/traces',
            '{"data": "test"}',
            ['Content-Type: application/json'],
        );

        static::assertSame('http://localhost:4318/v1/traces', $curlOptions[CURLOPT_URL]);
        static::assertTrue($curlOptions[CURLOPT_POST]);
        static::assertSame('{"data": "test"}', $curlOptions[CURLOPT_POSTFIELDS]);
        static::assertTrue($curlOptions[CURLOPT_RETURNTRANSFER]);
        static::assertSame(AsyncCurlTransportOptions::DEFAULT_TIMEOUT_MS, $curlOptions[CURLOPT_TIMEOUT_MS]);
        static::assertSame(
            AsyncCurlTransportOptions::DEFAULT_CONNECT_TIMEOUT_MS,
            $curlOptions[CURLOPT_CONNECTTIMEOUT_MS],
        );
        static::assertSame(['Content-Type: application/json'], $curlOptions[CURLOPT_HTTPHEADER]);
        static::assertTrue($curlOptions[CURLOPT_FOLLOWLOCATION]);
        static::assertSame(3, $curlOptions[CURLOPT_MAXREDIRS]);
        static::assertTrue($curlOptions[CURLOPT_SSL_VERIFYPEER]);
        static::assertSame(2, $curlOptions[CURLOPT_SSL_VERIFYHOST]);
    }

    public function test_to_curl_options_with_all_settings(): void
    {
        $options = (new AsyncCurlTransportOptions())
            ->withTimeout(2000)
            ->withConnectTimeout(500)
            ->withSslVerification(false, false)
            ->withSslCertificate('/path/to/cert.pem', '/path/to/key.pem')
            ->withCaInfo('/path/to/ca.crt')
            ->withProxy('http://proxy:8080')
            ->withCompression();

        $curlOptions = $options->toCurlOptions('http://example.com', 'body', ['Header: value']);

        static::assertSame(2000, $curlOptions[CURLOPT_TIMEOUT_MS]);
        static::assertSame(500, $curlOptions[CURLOPT_CONNECTTIMEOUT_MS]);
        static::assertFalse($curlOptions[CURLOPT_SSL_VERIFYPEER]);
        static::assertSame(0, $curlOptions[CURLOPT_SSL_VERIFYHOST]);
        static::assertSame('/path/to/cert.pem', $curlOptions[CURLOPT_SSLCERT]);
        static::assertSame('/path/to/key.pem', $curlOptions[CURLOPT_SSLKEY]);
        static::assertSame('/path/to/ca.crt', $curlOptions[CURLOPT_CAINFO]);
        static::assertSame('http://proxy:8080', $curlOptions[CURLOPT_PROXY]);
        static::assertSame('', $curlOptions[CURLOPT_ENCODING]);
    }

    public function test_with_ca_info(): void
    {
        $options = (new AsyncCurlTransportOptions())->withCaInfo('/path/to/ca-bundle.crt');

        static::assertSame('/path/to/ca-bundle.crt', $options->caInfoPath());
    }

    public function test_with_compression_disabled(): void
    {
        $options = (new AsyncCurlTransportOptions())
            ->withCompression(true)
            ->withCompression(false);

        static::assertFalse($options->compression());
    }

    public function test_with_compression_enabled(): void
    {
        $options = (new AsyncCurlTransportOptions())->withCompression();

        static::assertTrue($options->compression());
    }

    public function test_with_connect_timeout(): void
    {
        $options = (new AsyncCurlTransportOptions())->withConnectTimeout(750);

        static::assertSame(750, $options->connectTimeoutMs());
    }

    public function test_with_connect_timeout_rejects_negative_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Connect timeout must be non-negative');

        (new AsyncCurlTransportOptions())->withConnectTimeout(-1);
    }

    public function test_with_follow_redirects_disabled(): void
    {
        $options = (new AsyncCurlTransportOptions())->withFollowRedirects(false);

        static::assertFalse($options->followRedirects());
    }

    public function test_with_follow_redirects_enabled(): void
    {
        $options = (new AsyncCurlTransportOptions())->withFollowRedirects(true, 5);

        static::assertTrue($options->followRedirects());
        static::assertSame(5, $options->maxRedirects());
    }

    public function test_with_follow_redirects_rejects_negative_max_redirects(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Max redirects must be non-negative');

        (new AsyncCurlTransportOptions())->withFollowRedirects(true, -1);
    }

    public function test_with_header(): void
    {
        $options = (new AsyncCurlTransportOptions())
            ->withHeader('Authorization', 'Bearer token')
            ->withHeader('X-Custom', 'value');

        static::assertSame(
            [
                'Authorization' => 'Bearer token',
                'X-Custom' => 'value',
            ],
            $options->headers(),
        );
    }

    public function test_with_headers(): void
    {
        $options = (new AsyncCurlTransportOptions())->withHeaders([
            'Authorization' => 'Bearer token',
            'X-Custom' => 'value',
        ]);

        static::assertSame(
            [
                'Authorization' => 'Bearer token',
                'X-Custom' => 'value',
            ],
            $options->headers(),
        );
    }

    public function test_with_headers_replaces_previous_headers(): void
    {
        $options = (new AsyncCurlTransportOptions())
            ->withHeader('Old-Header', 'old-value')
            ->withHeaders(['New-Header' => 'new-value']);

        static::assertSame(['New-Header' => 'new-value'], $options->headers());
    }

    public function test_with_proxy(): void
    {
        $options = (new AsyncCurlTransportOptions())->withProxy('http://proxy:8080');

        static::assertSame('http://proxy:8080', $options->proxy());
    }

    public function test_with_shutdown_timeout(): void
    {
        $options = (new AsyncCurlTransportOptions())->withShutdownTimeout(7500);

        static::assertSame(7500, $options->shutdownTimeoutMs());
    }

    public function test_with_shutdown_timeout_rejects_negative_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Shutdown timeout must be non-negative');

        (new AsyncCurlTransportOptions())->withShutdownTimeout(-1);
    }

    public function test_with_ssl_certificate(): void
    {
        $options = (new AsyncCurlTransportOptions())->withSslCertificate('/path/to/cert.pem');

        static::assertSame('/path/to/cert.pem', $options->sslCertPath());
        static::assertNull($options->sslKeyPath());
    }

    public function test_with_ssl_certificate_and_key(): void
    {
        $options = (new AsyncCurlTransportOptions())->withSslCertificate('/path/to/cert.pem', '/path/to/key.pem');

        static::assertSame('/path/to/cert.pem', $options->sslCertPath());
        static::assertSame('/path/to/key.pem', $options->sslKeyPath());
    }

    public function test_with_ssl_verification_disabled(): void
    {
        $options = (new AsyncCurlTransportOptions())->withSslVerification(false, false);

        static::assertFalse($options->sslVerifyPeer());
        static::assertFalse($options->sslVerifyHost());
    }

    public function test_with_ssl_verification_enabled(): void
    {
        $options = (new AsyncCurlTransportOptions())->withSslVerification(true, true);

        static::assertTrue($options->sslVerifyPeer());
        static::assertTrue($options->sslVerifyHost());
    }

    public function test_with_timeout(): void
    {
        $options = (new AsyncCurlTransportOptions())->withTimeout(2500);

        static::assertSame(2500, $options->timeoutMs());
    }

    public function test_with_timeout_rejects_negative_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Timeout must be non-negative');

        (new AsyncCurlTransportOptions())->withTimeout(-1);
    }
}
