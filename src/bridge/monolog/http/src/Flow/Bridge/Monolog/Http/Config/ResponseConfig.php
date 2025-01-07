<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Config;

final readonly class ResponseConfig
{
    /**
     * @param bool $withReasonPhrase
     * @param bool $withStatus
     * @param bool $withBody
     * @param int $bodySizeLimit
     * @param array $withoutStatusCodes<int>
     * @param array $headers<string>
     */
    public function __construct(
        private bool $withReasonPhrase = true,
        private bool $withStatus = true,
        private bool $withBody = false,
        private int $bodySizeLimit = 1024 * 1024 * 32,
        private array $withoutStatusCodes = [],
        private array $headers = ['cache-control', 'location', 'set-cookie', 'server', 'expires', 'content-type', 'content-length', 'last-modified', 'kee-alive', 'referrer-policy', 'etag'],
    ) {

    }

    public function bodySizeLimit() : int
    {
        return $this->bodySizeLimit;
    }

    /**
     * @return array<int>
     */
    public function excludeStatusCodes() : array
    {
        return $this->withoutStatusCodes;
    }

    public function includeBody() : bool
    {
        return $this->withBody;
    }

    /**
     * @return array<string>
     */
    public function includeHeaders() : array
    {
        return $this->headers;
    }

    public function includeReasonPhrase() : bool
    {
        return $this->withReasonPhrase;
    }

    public function includeStatus() : bool
    {
        return $this->withStatus;
    }
}
