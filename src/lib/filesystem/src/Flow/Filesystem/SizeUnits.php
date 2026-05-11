<?php

declare(strict_types=1);

namespace Flow\Filesystem;

final class SizeUnits
{
    public const int GiB_SIZE = 1073741824;

    public const int KiB_SIZE = 1024;

    public const int MiB_SIZE = 1048576;

    public static function gbToBytes(int $gb): int
    {
        return $gb * self::GiB_SIZE;
    }

    /**
     * Format a byte count as a human-readable string (binary units: B, KiB, MiB, GiB, TiB, PiB).
     * The tail arguments mirror PHP's number_format signature for the fractional part.
     *
     * @param null|int $bytes size in bytes, or null for "unknown" (returns $null)
     * @param int $decimals Number of fractional digits for non-byte units. Ignored for raw bytes.
     * @param string $decimalSeparator decimal separator (passed to number_format)
     * @param string $thousandsSeparator thousands separator for the integer part (passed to number_format)
     * @param string $null placeholder returned when $bytes is null
     */
    public static function humanReadable(
        ?int $bytes,
        int $decimals = 2,
        string $decimalSeparator = '.',
        string $thousandsSeparator = ',',
        string $null = '-',
    ): string {
        if ($bytes === null) {
            return $null;
        }

        if ($bytes < self::KiB_SIZE) {
            return \number_format($bytes, 0, $decimalSeparator, $thousandsSeparator) . ' B';
        }

        $units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
        $value = (float) $bytes;
        $unit = 'B';

        foreach ($units as $candidate) {
            $value /= 1024;
            $unit = $candidate;

            if ($value < 1024) {
                break;
            }
        }

        return \number_format($value, $decimals, $decimalSeparator, $thousandsSeparator) . ' ' . $unit;
    }

    public static function kbToBytes(int $kb): int
    {
        return $kb * self::KiB_SIZE;
    }

    public static function mbToBytes(int $mb): int
    {
        return $mb * self::MiB_SIZE;
    }
}
