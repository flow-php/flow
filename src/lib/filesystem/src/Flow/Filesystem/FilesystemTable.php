<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Telemetry\FilesystemTelemetryConfig;
use Flow\Filesystem\Telemetry\TraceableFilesystem;

use function array_key_exists;
use function array_values;
use function Flow\Filesystem\DSL\traceable_filesystem;

final class FilesystemTable
{
    /**
     * @var array<string, Filesystem> keyed by mount protocol
     */
    private array $mounts = [];

    private ?FilesystemTelemetryConfig $telemetryConfig = null;

    public function __construct(Filesystem ...$filesystems)
    {
        foreach ($filesystems as $filesystem) {
            $this->mount($filesystem);
        }
    }

    /**
     * @return array<Filesystem>
     */
    public function filesystems(): array
    {
        return array_values($this->mounts);
    }

    public function for(Path|string $protocol): Filesystem
    {
        $name = $protocol instanceof Path ? $protocol->protocol() : $protocol;

        if (!array_key_exists($name, $this->mounts)) {
            throw new InvalidArgumentException("Filesystem with protocol {$name} is not mounted.");
        }

        return $this->mounts[$name];
    }

    public function mount(Filesystem $filesystem): void
    {
        $protocol = $filesystem->mount()->protocol;

        if (array_key_exists($protocol, $this->mounts)) {
            throw new InvalidArgumentException("Mount '{$protocol}' is already registered.");
        }

        $this->mounts[$protocol] = $this->telemetryConfig !== null && !$filesystem instanceof TraceableFilesystem
            ? traceable_filesystem($filesystem, $this->telemetryConfig)
            : $filesystem;
    }

    public function unmount(Filesystem $filesystem): void
    {
        $protocol = $filesystem->mount()->protocol;

        if (!array_key_exists($protocol, $this->mounts)) {
            throw new InvalidArgumentException("Filesystem with protocol {$protocol} is not mounted.");
        }

        unset($this->mounts[$protocol]);
    }

    public function withTelemetry(FilesystemTelemetryConfig $config): self
    {
        $this->telemetryConfig = $config;

        foreach ($this->mounts as $protocol => $filesystem) {
            if ($filesystem instanceof TraceableFilesystem) {
                continue;
            }

            $this->mounts[$protocol] = traceable_filesystem($filesystem, $config);
        }

        return $this;
    }
}
