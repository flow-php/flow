<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem;

use Flow\Filesystem\{Filesystem, Protocol};

interface FilesystemFactory
{
    /**
     * @param array<string, mixed> $config
     */
    public function create(Protocol $protocol, array $config) : Filesystem;

    public function protocol() : Protocol;
}
