<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use function Flow\Filesystem\DSL\traceable_filesystem;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Telemetry\{FilesystemTelemetryConfig, TraceableFilesystem};

final class FilesystemTable
{
    /**
     * @var array<string, Filesystem>
     */
    private array $fstab;

    private ?FilesystemTelemetryConfig $telemetryConfig = null;

    public function __construct(Filesystem ...$filesystems)
    {
        $fstab = [];

        foreach ($filesystems as $filesystem) {
            $fstab[$filesystem->protocol()->name] = $filesystem;
        }

        $this->fstab = $fstab;
    }

    /**
     * @return array<Filesystem>
     */
    public function filesystems() : array
    {
        return array_values($this->fstab);
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

        $this->fstab[$filesystem->protocol()->name] = $this->telemetryConfig
            ? $filesystem instanceof TraceableFilesystem
                ? $filesystem
                : traceable_filesystem($filesystem, $this->telemetryConfig)
            : $filesystem;
    }

    public function unmount(Filesystem $filesystem) : void
    {
        if (!isset($this->fstab[$filesystem->protocol()->name])) {
            throw new InvalidArgumentException("Filesystem with protocol {$filesystem->protocol()->name} is not mounted.");
        }

        unset($this->fstab[$filesystem->protocol()->name]);
    }

    public function withTelemetry(FilesystemTelemetryConfig $config) : self
    {
        $this->telemetryConfig = $config;

        foreach ($this->fstab as $protocol => $filesystem) {
            if ($filesystem instanceof TraceableFilesystem) {
                continue;
            }

            $this->fstab[$protocol] = traceable_filesystem($filesystem, $config);
        }

        return $this;
    }
}
