<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory;

use function Flow\Filesystem\DSL\native_local_filesystem;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\{Filesystem, Protocol};

final class NativeLocalFilesystemFactory implements FilesystemFactory
{
    public function create(Protocol $protocol, array $config) : Filesystem
    {
        if (!$this->protocol()->is($protocol->name)) {
            throw new InvalidArgumentException(\sprintf(
                'Filesystem factory for protocol "%s" cannot create filesystem for protocol "%s".',
                $this->protocol()->name,
                $protocol->name,
            ));
        }

        if ($config !== []) {
            throw new InvalidArgumentException(\sprintf(
                'Filesystem factory for protocol "%s" does not accept any options. Unknown keys: [%s].',
                $this->protocol()->name,
                \implode(', ', \array_keys($config)),
            ));
        }

        return native_local_filesystem();
    }

    public function protocol() : Protocol
    {
        return new Protocol('file');
    }
}
