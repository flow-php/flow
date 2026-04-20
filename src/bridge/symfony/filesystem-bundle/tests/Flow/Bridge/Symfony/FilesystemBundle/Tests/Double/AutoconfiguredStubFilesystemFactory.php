<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Double;

use Flow\Bridge\Symfony\FilesystemBundle\Attribute\AsFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\Filesystem;

#[AsFilesystemFactory(type: 'file')]
final readonly class AutoconfiguredStubFilesystemFactory implements FilesystemFactory
{
    public function create(string $protocol, array $config) : Filesystem
    {
        throw new \RuntimeException('AutoconfiguredStubFilesystemFactory is a fixture and cannot create filesystems.');
    }

    public function type() : string
    {
        return 'file';
    }
}
