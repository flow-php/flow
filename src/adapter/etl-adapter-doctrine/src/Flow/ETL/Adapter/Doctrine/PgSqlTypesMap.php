<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\Types\Type;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;

final readonly class PgSqlTypesMap implements NativeTypes
{
    public function toFlowType(int|string|null $native): ?Type
    {
        return match ($native) {
            'int2', 'int4', 'int8' => type_integer(),
            'float4', 'float8', 'numeric' => type_float(),
            'bool' => type_boolean(),
            'text', 'varchar', 'bpchar', 'name', 'bytea' => type_string(),
            'date' => type_date(),
            'time' => type_time(),
            'timestamp', 'timestamptz' => type_datetime(),
            'uuid' => type_uuid(),
            'json', 'jsonb' => type_json(),
            'xml' => type_xml(),
            default => null,
        };
    }
}
