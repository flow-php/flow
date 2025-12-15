<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types;

enum PostgreSqlVersion : int
{
    case V12 = 120000;
    case V13 = 130000;
    case V14 = 140000;
    case V15 = 150000;
    case V16 = 160000;
    case V17 = 170000;

    /**
     * Create version from server_version_num.
     */
    public static function fromVersionNum(int $versionNum) : self
    {
        return match (true) {
            $versionNum >= 170000 => self::V17,
            $versionNum >= 160000 => self::V16,
            $versionNum >= 150000 => self::V15,
            $versionNum >= 140000 => self::V14,
            $versionNum >= 130000 => self::V13,
            default => self::V12,
        };
    }

    public function supportsJsonb() : bool
    {
        return true;
    }

    public function supportsJsonPath() : bool
    {
        return $this->value >= self::V12->value; // Added in PostgreSQL 12
    }

    public function supportsMultirange() : bool
    {
        return $this->value >= self::V14->value; // Added in PostgreSQL 14
    }
}
