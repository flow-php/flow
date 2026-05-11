<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\Filesystem;

use function Flow\Filesystem\DSL\memory_filesystem;

final class MemoryFilesystemFactory implements FilesystemFactory
{
    public function create(string $protocol, array $config): Filesystem
    {
        if ($config !== []) {
            throw new InvalidArgumentException(\sprintf(
                'Filesystem factory for type "%s" does not accept any options. Unknown keys: [%s].',
                $this->type(),
                \implode(', ', \array_keys($config)),
            ));
        }

        return memory_filesystem($protocol);
    }

    public function type(): string
    {
        return 'memory';
    }
}
