<?php

declare(strict_types=1);

namespace Flow\Filesystem;

final class SizeUnits
{
    public const GiB_SIZE = 1073741824;

    public const KiB_SIZE = 1024;

    public const MiB_SIZE = 1048576;

    public static function gbToBytes(int $gb) : int
    {
        return $gb * self::GiB_SIZE;
    }

    public static function kbToBytes(int $kb) : int
    {
        return $kb * self::KiB_SIZE;
    }

    public static function mbToBytes(int $mb) : int
    {
        return $mb * self::MiB_SIZE;
    }
}
