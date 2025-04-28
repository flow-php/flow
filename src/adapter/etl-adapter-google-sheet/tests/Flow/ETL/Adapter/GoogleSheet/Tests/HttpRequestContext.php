<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests;

final class HttpRequestContext
{
    public function __construct(
        public string $method,
        public string $url,
        public ?string $body = null,
    ) {
    }
}
