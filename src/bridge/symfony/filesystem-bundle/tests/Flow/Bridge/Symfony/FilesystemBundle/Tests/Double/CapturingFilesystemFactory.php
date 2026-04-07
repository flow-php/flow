<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Double;

use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\{Filesystem, Protocol};

final class CapturingFilesystemFactory implements FilesystemFactory
{
    /** @var list<array{protocol: string, config: array<string, mixed>}> */
    public array $calls = [];

    public function __construct(
        private readonly string $protocolName,
        private readonly Filesystem $filesystem,
    ) {
    }

    public function create(Protocol $protocol, array $config) : Filesystem
    {
        $this->calls[] = ['protocol' => $protocol->name, 'config' => $config];

        return $this->filesystem;
    }

    public function protocol() : Protocol
    {
        return new Protocol($this->protocolName);
    }
}
