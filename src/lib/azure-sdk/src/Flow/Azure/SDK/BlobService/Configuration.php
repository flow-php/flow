<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService;

final class Configuration
{
    public function __construct(
        public readonly string $account,
        public readonly string $container,
    ) {
    }
}
