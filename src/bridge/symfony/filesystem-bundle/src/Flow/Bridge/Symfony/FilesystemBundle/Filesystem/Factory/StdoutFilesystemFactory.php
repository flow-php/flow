<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory;

use function Flow\Filesystem\DSL\stdout_filesystem;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\Filesystem;

final class StdoutFilesystemFactory implements FilesystemFactory
{
    public function create(string $protocol, array $config) : Filesystem
    {
        if ($config !== []) {
            throw new InvalidArgumentException(\sprintf(
                'Filesystem factory for type "%s" does not accept any options. Unknown keys: [%s].',
                $this->type(),
                \implode(', ', \array_keys($config)),
            ));
        }

        return stdout_filesystem($protocol);
    }

    public function type() : string
    {
        return 'stdout';
    }
}
