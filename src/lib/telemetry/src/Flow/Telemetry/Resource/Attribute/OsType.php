<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * Operating system type values following OpenTelemetry semantic conventions.
 *
 * These values correspond to the `os.type` attribute as defined in the
 * OpenTelemetry semantic conventions specification.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/resource/os/
 */
enum OsType : string
{
    case AIX = 'aix';

    case DARWIN = 'darwin';

    case DRAGONFLYBSD = 'dragonflybsd';

    case FREEBSD = 'freebsd';

    case HPUX = 'hpux';

    case LINUX = 'linux';

    case NETBSD = 'netbsd';

    case OPENBSD = 'openbsd';

    case SOLARIS = 'solaris';

    case WINDOWS = 'windows';

    case ZOS = 'z_os';

    /**
     * Detect the current operating system type.
     *
     * Uses PHP_OS_FAMILY constant to determine the OS type.
     *
     * @return null|self The detected OS type, or null if not recognized
     */
    public static function detect() : ?self
    {
        return match (PHP_OS_FAMILY) {
            'Darwin' => self::DARWIN,
            'Windows' => self::WINDOWS,
            'Linux' => self::LINUX,
            'BSD' => self::determineBsdVariant(),
            'Solaris' => self::SOLARIS,
            default => null,
        };
    }

    private static function determineBsdVariant() : self
    {
        $os = \strtolower(\php_uname('s'));

        return match (true) {
            \str_contains($os, 'freebsd') => self::FREEBSD,
            \str_contains($os, 'openbsd') => self::OPENBSD,
            \str_contains($os, 'netbsd') => self::NETBSD,
            \str_contains($os, 'dragonfly') => self::DRAGONFLYBSD,
            default => self::FREEBSD,
        };
    }
}
