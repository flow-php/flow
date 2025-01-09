<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\URLFactory;

use Flow\Azure\SDK\BlobService\{Configuration};
use Flow\Azure\SDK\URLFactory;

final readonly class AzureURLFactory implements URLFactory
{
    public function __construct(private string $host = 'blob.core.windows.net')
    {

    }

    public function create(Configuration $configuration, ?string $path = null, array $queryParameters = []) : string
    {
        return \sprintf(
            'https://%s.%s/%s%s%s',
            $configuration->account,
            $this->host,
            $configuration->container,
            $path ? ('/' . \trim($path, '/')) : '',
            $queryParameters ? ('?' . \http_build_query($queryParameters)) : ''
        );
    }
}
