<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Double;

use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\Filesystem;

final class CapturingFilesystemFactory implements FilesystemFactory
{
    /** @var list<array{mount: string, config: array<string, mixed>}> */
    public array $calls = [];

    public function __construct(
        private readonly string $type,
        private readonly Filesystem $filesystem,
    ) {}

    public function create(string $protocol, array $config): Filesystem
    {
        $this->calls[] = ['mount' => $protocol, 'config' => $config];

        return $this->filesystem;
    }

    public function type(): string
    {
        return $this->type;
    }
}
