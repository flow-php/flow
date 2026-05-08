<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Transport;

use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_curl_options;
use Flow\Bridge\Telemetry\OTLP\Transport\CurlTransportOptions;
use PHPUnit\Framework\TestCase;

final class CurlTransportOptionsTest extends TestCase
{
    public function test_default_options() : void
    {
        $options = new CurlTransportOptions();

        self::assertSame(CurlTransportOptions::DEFAULT_TIMEOUT_MS, $options->timeoutMs());
        self::assertSame(CurlTransportOptions::DEFAULT_CONNECT_TIMEOUT_MS, $options->connectTimeoutMs());
        self::assertSame([], $options->headers());
        self::assertTrue($options->followRedirects());
        self::assertSame(3, $options->maxRedirects());
        self::assertTrue($options->sslVerifyPeer());
        self::assertTrue($options->sslVerifyHost());
        self::assertNull($options->sslCertPath());
        self::assertNull($options->sslKeyPath());
        self::assertNull($options->caInfoPath());
        self::assertNull($options->proxy());
        self::assertFalse($options->compression());
    }

    public function test_default_shutdown_timeout() : void
    {
        $options = new CurlTransportOptions();

        self::assertSame(CurlTransportOptions::DEFAULT_SHUTDOWN_TIMEOUT_MS, $options->shutdownTimeoutMs());
    }

    public function test_dsl_function_creates_options() : void
    {
        $options = otlp_curl_options();

        self::assertInstanceOf(CurlTransportOptions::class, $options);
        self::assertSame(CurlTransportOptions::DEFAULT_TIMEOUT_MS, $options->timeoutMs());
    }

    public function test_fluent_chaining() : void
    {
        $options = otlp_curl_options()
            ->withTimeout(2000)
            ->withConnectTimeout(500)
            ->withHeader('Authorization', 'Bearer token')
            ->withFollowRedirects(true, 5)
            ->withSslVerification(true)
            ->withProxy('http://proxy:8080')
            ->withCompression();

        self::assertSame(2000, $options->timeoutMs());
        self::assertSame(500, $options->connectTimeoutMs());
        self::assertSame(['Authorization' => 'Bearer token'], $options->headers());
        self::assertTrue($options->followRedirects());
        self::assertSame(5, $options->maxRedirects());
        self::assertTrue($options->sslVerifyPeer());
        self::assertSame('http://proxy:8080', $options->proxy());
        self::assertTrue($options->compression());
    }

    public function test_to_curl_options_basic() : void
    {
        $options = new CurlTransportOptions();

        $curlOptions = $options->toCurlOptions(
            'http://localhost:4318/v1/traces',
            '{"data": "test"}',
            ['Content-Type: application/json']
        );

        self::assertSame('http://localhost:4318/v1/traces', $curlOptions[\CURLOPT_URL]);
        self::assertTrue($curlOptions[\CURLOPT_POST]);
        self::assertSame('{"data": "test"}', $curlOptions[\CURLOPT_POSTFIELDS]);
        self::assertTrue($curlOptions[\CURLOPT_RETURNTRANSFER]);
        self::assertSame(CurlTransportOptions::DEFAULT_TIMEOUT_MS, $curlOptions[\CURLOPT_TIMEOUT_MS]);
        self::assertSame(CurlTransportOptions::DEFAULT_CONNECT_TIMEOUT_MS, $curlOptions[\CURLOPT_CONNECTTIMEOUT_MS]);
        self::assertSame(['Content-Type: application/json'], $curlOptions[\CURLOPT_HTTPHEADER]);
        self::assertTrue($curlOptions[\CURLOPT_FOLLOWLOCATION]);
        self::assertSame(3, $curlOptions[\CURLOPT_MAXREDIRS]);
        self::assertTrue($curlOptions[\CURLOPT_SSL_VERIFYPEER]);
        self::assertSame(2, $curlOptions[\CURLOPT_SSL_VERIFYHOST]);
    }

    public function test_to_curl_options_with_all_settings() : void
    {
        $options = (new CurlTransportOptions())
            ->withTimeout(2000)
            ->withConnectTimeout(500)
            ->withSslVerification(false, false)
            ->withSslCertificate('/path/to/cert.pem', '/path/to/key.pem')
            ->withCaInfo('/path/to/ca.crt')
            ->withProxy('http://proxy:8080')
            ->withCompression();

        $curlOptions = $options->toCurlOptions('http://example.com', 'body', ['Header: value']);

        self::assertSame(2000, $curlOptions[\CURLOPT_TIMEOUT_MS]);
        self::assertSame(500, $curlOptions[\CURLOPT_CONNECTTIMEOUT_MS]);
        self::assertFalse($curlOptions[\CURLOPT_SSL_VERIFYPEER]);
        self::assertSame(0, $curlOptions[\CURLOPT_SSL_VERIFYHOST]);
        self::assertSame('/path/to/cert.pem', $curlOptions[\CURLOPT_SSLCERT]);
        self::assertSame('/path/to/key.pem', $curlOptions[\CURLOPT_SSLKEY]);
        self::assertSame('/path/to/ca.crt', $curlOptions[\CURLOPT_CAINFO]);
        self::assertSame('http://proxy:8080', $curlOptions[\CURLOPT_PROXY]);
        self::assertSame('', $curlOptions[\CURLOPT_ENCODING]);
    }

    public function test_with_ca_info() : void
    {
        $options = (new CurlTransportOptions())->withCaInfo('/path/to/ca-bundle.crt');

        self::assertSame('/path/to/ca-bundle.crt', $options->caInfoPath());
    }

    public function test_with_compression_disabled() : void
    {
        $options = (new CurlTransportOptions())
            ->withCompression(true)
            ->withCompression(false);

        self::assertFalse($options->compression());
    }

    public function test_with_compression_enabled() : void
    {
        $options = (new CurlTransportOptions())->withCompression();

        self::assertTrue($options->compression());
    }

    public function test_with_connect_timeout() : void
    {
        $options = (new CurlTransportOptions())->withConnectTimeout(750);

        self::assertSame(750, $options->connectTimeoutMs());
    }

    public function test_with_connect_timeout_rejects_negative_value() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Connect timeout must be non-negative');

        (new CurlTransportOptions())->withConnectTimeout(-1);
    }

    public function test_with_follow_redirects_disabled() : void
    {
        $options = (new CurlTransportOptions())->withFollowRedirects(false);

        self::assertFalse($options->followRedirects());
    }

    public function test_with_follow_redirects_enabled() : void
    {
        $options = (new CurlTransportOptions())->withFollowRedirects(true, 5);

        self::assertTrue($options->followRedirects());
        self::assertSame(5, $options->maxRedirects());
    }

    public function test_with_follow_redirects_rejects_negative_max_redirects() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Max redirects must be non-negative');

        (new CurlTransportOptions())->withFollowRedirects(true, -1);
    }

    public function test_with_header() : void
    {
        $options = (new CurlTransportOptions())
            ->withHeader('Authorization', 'Bearer token')
            ->withHeader('X-Custom', 'value');

        self::assertSame([
            'Authorization' => 'Bearer token',
            'X-Custom' => 'value',
        ], $options->headers());
    }

    public function test_with_headers() : void
    {
        $options = (new CurlTransportOptions())->withHeaders([
            'Authorization' => 'Bearer token',
            'X-Custom' => 'value',
        ]);

        self::assertSame([
            'Authorization' => 'Bearer token',
            'X-Custom' => 'value',
        ], $options->headers());
    }

    public function test_with_headers_replaces_previous_headers() : void
    {
        $options = (new CurlTransportOptions())
            ->withHeader('Old-Header', 'old-value')
            ->withHeaders(['New-Header' => 'new-value']);

        self::assertSame(['New-Header' => 'new-value'], $options->headers());
    }

    public function test_with_proxy() : void
    {
        $options = (new CurlTransportOptions())->withProxy('http://proxy:8080');

        self::assertSame('http://proxy:8080', $options->proxy());
    }

    public function test_with_shutdown_timeout() : void
    {
        $options = (new CurlTransportOptions())->withShutdownTimeout(7500);

        self::assertSame(7500, $options->shutdownTimeoutMs());
    }

    public function test_with_shutdown_timeout_rejects_negative_value() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Shutdown timeout must be non-negative');

        (new CurlTransportOptions())->withShutdownTimeout(-1);
    }

    public function test_with_ssl_certificate() : void
    {
        $options = (new CurlTransportOptions())->withSslCertificate('/path/to/cert.pem');

        self::assertSame('/path/to/cert.pem', $options->sslCertPath());
        self::assertNull($options->sslKeyPath());
    }

    public function test_with_ssl_certificate_and_key() : void
    {
        $options = (new CurlTransportOptions())->withSslCertificate('/path/to/cert.pem', '/path/to/key.pem');

        self::assertSame('/path/to/cert.pem', $options->sslCertPath());
        self::assertSame('/path/to/key.pem', $options->sslKeyPath());
    }

    public function test_with_ssl_verification_disabled() : void
    {
        $options = (new CurlTransportOptions())->withSslVerification(false, false);

        self::assertFalse($options->sslVerifyPeer());
        self::assertFalse($options->sslVerifyHost());
    }

    public function test_with_ssl_verification_enabled() : void
    {
        $options = (new CurlTransportOptions())->withSslVerification(true, true);

        self::assertTrue($options->sslVerifyPeer());
        self::assertTrue($options->sslVerifyHost());
    }

    public function test_with_timeout() : void
    {
        $options = (new CurlTransportOptions())->withTimeout(2500);

        self::assertSame(2500, $options->timeoutMs());
    }

    public function test_with_timeout_rejects_negative_value() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Timeout must be non-negative');

        (new CurlTransportOptions())->withTimeout(-1);
    }
}
