<?php

declare(strict_types=1);

namespace Flow\Telemetry;

use Flow\Telemetry\Sanitizer\UrlSanitizer;

use function parse_url;

final readonly class ClientUrlAttributes
{
    public function __construct(
        private UrlSanitizer $urlSanitizer = new UrlSanitizer(),
    ) {}

    /**
     * @return array<string, int|string>
     */
    public function of(string $url): array
    {
        $parsedUrl = parse_url($url);
        $scheme = $parsedUrl['scheme'] ?? 'http';

        return [
            SemConvAttributes::URL_FULL => $this->urlSanitizer->url($url),
            SemConvAttributes::URL_SCHEME => $scheme,
            SemConvAttributes::SERVER_ADDRESS => $parsedUrl['host'] ?? 'unknown',
            // OTEL HTTP semconv: server.port is Required on client spans; fall back to the
            // scheme default when the URL carries no explicit port.
            SemConvAttributes::SERVER_PORT => $parsedUrl['port'] ?? ($scheme === 'https' ? 443 : 80),
        ];
    }
}
