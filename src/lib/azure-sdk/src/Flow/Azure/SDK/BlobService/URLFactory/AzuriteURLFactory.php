<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\URLFactory;

use Flow\Azure\SDK\BlobService\{Configuration};
use Flow\Azure\SDK\URLFactory;

final class AzuriteURLFactory implements URLFactory
{
    public function __construct(private readonly string $host = '127.0.0.1', private readonly string $port = '10000', private readonly bool $secure = false)
    {
    }

    public function create(Configuration $configuration, ?string $path = null, array $queryParameters = []) : string
    {
        return \sprintf(
            '%s://%s:%s/%s/%s%s%s',
            $this->secure ? 'https' : 'http',
            $this->host,
            $this->port,
            $configuration->account,
            $configuration->container,
            $path ? ('/' . \trim($path, '/')) : '',
            $queryParameters ? ('?' . \http_build_query($queryParameters)) : ''
        );
    }
}
