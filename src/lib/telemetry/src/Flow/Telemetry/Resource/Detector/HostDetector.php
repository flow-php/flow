<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Attribute\HostAttribute;
use Flow\Telemetry\Resource\ResourceDetector;

/**
 * Detects host information.
 *
 * Detects the following attributes:
 * - host.name: The hostname of the machine
 * - host.arch: The CPU architecture (amd64, arm64, etc.)
 * - host.id: Unique host identifier (from /etc/machine-id on Linux)
 *
 * Example output:
 * ```
 * host.name: my-laptop
 * host.arch: arm64
 * host.id: 123e4567-e89b-12d3-a456-426614174000
 * ```
 */
final readonly class HostDetector implements ResourceDetector
{
    public function detect(): Resource
    {
        $attributes = [];

        $hostname = \php_uname('n');

        if ($hostname !== '') {
            $attributes[HostAttribute::NAME->value] = $hostname;
        }

        $arch = $this->detectArchitecture();

        if ($arch !== null) {
            $attributes[HostAttribute::ARCH->value] = $arch;
        }

        $machineId = $this->detectMachineId();

        if ($machineId !== null) {
            $attributes[HostAttribute::ID->value] = $machineId;
        }

        return Resource::create($attributes);
    }

    private function detectArchitecture(): ?string
    {
        $machine = \strtolower(\php_uname('m'));

        return match (true) {
            $machine === 'x86_64' || $machine === 'amd64' => 'amd64',
            $machine === 'aarch64' || $machine === 'arm64' => 'arm64',
            \str_starts_with($machine, 'arm') => 'arm32',
            $machine === 'i386' || $machine === 'i686' || $machine === 'x86' => 'x86',
            $machine === 'ia64' => 'ia64',
            $machine === 'ppc' || $machine === 'ppc32' || $machine === 'powerpc' => 'ppc32',
            $machine === 'ppc64' || $machine === 'ppc64le' => 'ppc64',
            $machine === 's390x' => 's390x',
            default => null,
        };
    }

    private function detectMachineId(): ?string
    {
        if (PHP_OS_FAMILY === 'Linux') {
            return $this->readLinuxMachineId();
        }

        if (PHP_OS_FAMILY === 'Darwin') {
            return $this->readDarwinMachineId();
        }

        return null;
    }

    private function readDarwinMachineId(): ?string
    {
        $output = @\shell_exec('ioreg -rd1 -c IOPlatformExpertDevice 2>/dev/null');

        if ($output === null || $output === false) {
            return null;
        }

        if (\preg_match('/"IOPlatformUUID"\s*=\s*"([^"]+)"/', $output, $matches)) {
            return $matches[1];
        }

        return null;
    }

    private function readLinuxMachineId(): ?string
    {
        $paths = [
            '/etc/machine-id',
            '/var/lib/dbus/machine-id',
        ];

        foreach ($paths as $path) {
            if (\is_readable($path)) {
                $content = @\file_get_contents($path);

                if ($content !== false) {
                    return \trim($content);
                }
            }
        }

        return null;
    }
}
