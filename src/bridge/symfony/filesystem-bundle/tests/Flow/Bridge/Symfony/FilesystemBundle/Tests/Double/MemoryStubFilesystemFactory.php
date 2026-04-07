<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Double;

use function Flow\Filesystem\DSL\memory_filesystem;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\{Filesystem, Protocol};

final readonly class MemoryStubFilesystemFactory implements FilesystemFactory
{
    public function __construct(private string $protocolName)
    {
    }

    public function create(Protocol $protocol, array $config) : Filesystem
    {
        return memory_filesystem();
    }

    public function protocol() : Protocol
    {
        return new Protocol($this->protocolName);
    }
}
