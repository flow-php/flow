<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Mother;

use Flow\ETL\Values\Schema;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_numeric_string;

final class BodySchemaMother
{
    /**
     * One column of every Flow type a parquet leaf reads as.
     */
    public static function everyLeafType(): Schema
    {
        return schema(
            int_schema('id', nullable: true),
            float_schema('price'),
            str_schema('name'),
            bool_schema('flag'),
            uuid_schema('uuid'),
            json_schema('doc'),
            datetime_schema('ts'),
            date_schema('day'),
            definition_from_type('big', type_numeric_string()),
        );
    }
}
