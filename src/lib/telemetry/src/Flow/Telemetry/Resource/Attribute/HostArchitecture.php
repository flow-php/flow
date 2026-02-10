<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * Host CPU architecture values following OpenTelemetry semantic conventions.
 *
 * These values correspond to the `host.arch` attribute as defined in the
 * OpenTelemetry semantic conventions specification.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/resource/host/
 */
enum HostArchitecture : string
{
    case AMD64 = 'amd64';

    case ARM32 = 'arm32';

    case ARM64 = 'arm64';

    case IA64 = 'ia64';

    case PPC32 = 'ppc32';

    case PPC64 = 'ppc64';

    case S390X = 's390x';

    case X86 = 'x86';

    /**
     * Detect the current host architecture.
     *
     * Uses php_uname('m') to determine the CPU architecture.
     *
     * @return null|self The detected architecture, or null if not recognized
     */
    public static function detect() : ?self
    {
        $machine = \strtolower(\php_uname('m'));

        return match (true) {
            $machine === 'x86_64' || $machine === 'amd64' => self::AMD64,
            $machine === 'aarch64' || $machine === 'arm64' => self::ARM64,
            \str_starts_with($machine, 'arm') => self::ARM32,
            $machine === 'i386' || $machine === 'i686' || $machine === 'x86' => self::X86,
            $machine === 'ia64' => self::IA64,
            $machine === 'ppc' || $machine === 'ppc32' || $machine === 'powerpc' => self::PPC32,
            $machine === 'ppc64' || $machine === 'ppc64le' => self::PPC64,
            $machine === 's390x' => self::S390X,
            default => null,
        };
    }
}
