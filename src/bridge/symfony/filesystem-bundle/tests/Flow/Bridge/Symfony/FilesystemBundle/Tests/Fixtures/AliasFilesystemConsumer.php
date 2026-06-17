<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures;

use Flow\Filesystem\Filesystem;

final readonly class AliasFilesystemConsumer
{
    public function __construct(
        public Filesystem $memory,
    ) {}
}
