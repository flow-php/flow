<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\Filesystem\Exception\InvalidArgumentException;

final class FilesystemTable
{
    /**
     * @var array<string, Filesystem>
     */
    private array $fstab;

    public function __construct(Filesystem ...$filesystems)
    {
        $fstab = [];

        foreach ($filesystems as $filesystem) {
            $fstab[$filesystem->protocol()->name] = $filesystem;
        }

        $this->fstab = $fstab;
    }

    public function for(Path|Protocol $path) : Filesystem
    {
        $protocol = $path instanceof Path ? $path->protocol() : $path;

        if (!\array_key_exists($protocol->name, $this->fstab)) {
            throw new InvalidArgumentException("Filesystem with protocol {$protocol->name} is not mounted.");
        }

        return $this->fstab[$protocol->name];
    }

    public function mount(Filesystem $filesystem) : void
    {
        if (isset($this->fstab[$filesystem->protocol()->name])) {
            throw new InvalidArgumentException("Filesystem with protocol {$filesystem->protocol()->name} is already mounted.");
        }

        $this->fstab[$filesystem->protocol()->name] = $filesystem;
    }

    public function unmount(Filesystem $filesystem) : void
    {
        if (!isset($this->fstab[$filesystem->protocol()->name])) {
            throw new InvalidArgumentException("Filesystem with protocol {$filesystem->protocol()->name} is not mounted.");
        }

        unset($this->fstab[$filesystem->protocol()->name]);
    }
}
