<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\ClientUrlAttributes;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\url_sanitizer;

final class ClientUrlAttributesTest extends TestCase
{
    public function test_default_sanitizer_redacts_sensitive_query_parameters(): void
    {
        static::assertSame(
            'https://h/x?access_token=REDACTED&fields=id',
            (new ClientUrlAttributes())->of('https://h/x?access_token=T&fields=id')['url.full'],
        );
    }

    public function test_full_url(): void
    {
        static::assertSame(
            [
                'url.full' => 'https://REDACTED:REDACTED@graph.facebook.com:8443/me?fb_exchange_token=REDACTED&fields=id',
                'url.scheme' => 'https',
                'server.address' => 'graph.facebook.com',
                'server.port' => 8443,
            ],
            (new ClientUrlAttributes(url_sanitizer(['fb_exchange_token'])))->of(
                'https://u:p@graph.facebook.com:8443/me?fb_exchange_token=T&fields=id',
            ),
        );
    }

    #[TestWith(['https://h/x', 'https', 443])]
    #[TestWith(['http://h/x', 'http', 80])]
    #[TestWith(['/x', 'http', 80])]
    public function test_scheme_and_port_fall_back_to_defaults(string $url, string $scheme, int $port): void
    {
        $attributes = (new ClientUrlAttributes())->of($url);

        static::assertSame($scheme, $attributes['url.scheme']);
        static::assertSame($port, $attributes['server.port']);
    }

    public function test_server_address_falls_back_to_unknown_without_host(): void
    {
        static::assertSame('unknown', (new ClientUrlAttributes())->of('/x')['server.address']);
    }
}
