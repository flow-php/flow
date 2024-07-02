<?php

declare(strict_types=1);

namespace Flow\Azure\SDK;

use Flow\Azure\SDK\BlobService\Configuration;

interface URLFactory
{
    public function create(Configuration $configuration, ?string $path = null, array $queryParameters = []) : string;
}
