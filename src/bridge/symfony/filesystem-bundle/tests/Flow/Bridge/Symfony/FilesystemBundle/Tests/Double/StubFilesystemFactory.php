<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Double;

use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\{Filesystem, Protocol};

final readonly class StubFilesystemFactory implements FilesystemFactory
{
    public function __construct(
        private string $protocolName,
        private ?Filesystem $filesystem = null,
    ) {
    }

    public function create(Protocol $protocol, array $config) : Filesystem
    {
        if ($this->filesystem === null) {
            throw new \RuntimeException('StubFilesystemFactory was not configured with a Filesystem instance.');
        }

        return $this->filesystem;
    }

    public function protocol() : Protocol
    {
        return new Protocol($this->protocolName);
    }
}
