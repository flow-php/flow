<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService;

final readonly class Configuration
{
    public function __construct(
        public string $account,
        public string $container,
    ) {
    }
}
