<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Config;

final class RequestConfig
{
    public function __construct(
        private readonly bool $withMethod = true,
        private readonly bool $withUri = true,
        private readonly bool $withBody = false,
        private readonly int $bodySizeLimit = 1024 * 1024 * 32,
        private readonly array $headers = ['host', 'accept', 'user-agent']
    ) {
    }

    public function bodySizeLimit() : int
    {
        return $this->bodySizeLimit;
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

    public function includeMethod() : bool
    {
        return $this->withMethod;
    }

    public function includeUri() : bool
    {
        return $this->withUri;
    }
}
