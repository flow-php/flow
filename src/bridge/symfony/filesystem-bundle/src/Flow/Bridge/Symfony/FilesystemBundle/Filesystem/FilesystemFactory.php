<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem;

use Flow\Filesystem\Filesystem;

interface FilesystemFactory
{
    /**
     * @param array<string, mixed> $config
     */
    public function create(string $protocol, array $config) : Filesystem;

    public function type() : string;
}
