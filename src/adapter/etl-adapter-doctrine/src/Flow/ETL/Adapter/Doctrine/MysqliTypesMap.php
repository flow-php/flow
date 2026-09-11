<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\Types\Type;

use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;

use const MYSQLI_TYPE_BLOB;
use const MYSQLI_TYPE_DATE;
use const MYSQLI_TYPE_DATETIME;
use const MYSQLI_TYPE_DECIMAL;
use const MYSQLI_TYPE_DOUBLE;
use const MYSQLI_TYPE_ENUM;
use const MYSQLI_TYPE_FLOAT;
use const MYSQLI_TYPE_INT24;
use const MYSQLI_TYPE_JSON;
use const MYSQLI_TYPE_LONG;
use const MYSQLI_TYPE_LONGLONG;
use const MYSQLI_TYPE_NEWDATE;
use const MYSQLI_TYPE_NEWDECIMAL;
use const MYSQLI_TYPE_SET;
use const MYSQLI_TYPE_SHORT;
use const MYSQLI_TYPE_STRING;
use const MYSQLI_TYPE_TIME;
use const MYSQLI_TYPE_TIMESTAMP;
use const MYSQLI_TYPE_TINY;
use const MYSQLI_TYPE_VAR_STRING;
use const MYSQLI_TYPE_YEAR;

final readonly class MysqliTypesMap implements NativeTypes
{
    /**
     * mysqli_result::fetch_fields() reports an integer type constant per column.
     * MYSQLI_TYPE_CHAR is an alias of MYSQLI_TYPE_TINY (both 1) - listing both is a duplicate-arm
     * fatal. MYSQLI_TYPE_VARCHAR does not exist in PHP 8.3. TINY maps to integer, not boolean:
     * the result route is told a type constant, not a display width, so tinyint(1) and tinyint(4)
     * are indistinguishable.
     *
     */
    public function toFlowType(int|string|null $native): ?Type
    {
        return match ($native) {
            MYSQLI_TYPE_TINY,
            MYSQLI_TYPE_SHORT,
            MYSQLI_TYPE_INT24,
            MYSQLI_TYPE_LONG,
            MYSQLI_TYPE_LONGLONG,
            MYSQLI_TYPE_YEAR,
                => type_integer(),
            MYSQLI_TYPE_FLOAT, MYSQLI_TYPE_DOUBLE, MYSQLI_TYPE_DECIMAL, MYSQLI_TYPE_NEWDECIMAL => type_float(),
            MYSQLI_TYPE_STRING,
            MYSQLI_TYPE_VAR_STRING,
            MYSQLI_TYPE_BLOB,
            MYSQLI_TYPE_ENUM,
            MYSQLI_TYPE_SET,
                => type_string(),
            MYSQLI_TYPE_DATE, MYSQLI_TYPE_NEWDATE => type_date(),
            MYSQLI_TYPE_TIME => type_time(),
            MYSQLI_TYPE_DATETIME, MYSQLI_TYPE_TIMESTAMP => type_datetime(),
            MYSQLI_TYPE_JSON => type_json(),
            default => null,
        };
    }
}
