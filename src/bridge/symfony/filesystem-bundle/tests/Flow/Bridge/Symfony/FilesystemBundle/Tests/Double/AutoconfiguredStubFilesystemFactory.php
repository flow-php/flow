<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Double;

use Flow\Bridge\Symfony\FilesystemBundle\Attribute\AsFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\{Filesystem, Protocol};

#[AsFilesystemFactory(protocol: 'stub-autoconfigured')]
final readonly class AutoconfiguredStubFilesystemFactory implements FilesystemFactory
{
    public function create(Protocol $protocol, array $config) : Filesystem
    {
        throw new \RuntimeException('AutoconfiguredStubFilesystemFactory is a fixture and cannot create filesystems.');
    }

    public function protocol() : Protocol
    {
        return new Protocol('stub-autoconfigured');
    }
}
