<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_union;

final class DefinitionMother
{
    /**
     * All on the same reference so they can be merged with each other.
     *
     * @return array<string, Definition<mixed>>
     */
    public static function oneOfEachType(string $ref = 'a'): array
    {
        return [
            'null' => null_schema($ref),
            'bool' => bool_schema($ref),
            'int' => int_schema($ref),
            'float' => float_schema($ref),
            'string' => string_schema($ref),
            'date' => date_schema($ref),
            'datetime' => datetime_schema($ref),
            'time' => time_schema($ref),
            'uuid' => uuid_schema($ref),
            'enum' => enum_schema($ref, BackedStringEnum::class),
            'json' => json_schema($ref),
            'struct' => structure_schema($ref, type_structure(['x' => type_integer()])),
            'list' => list_schema($ref, type_list(type_integer())),
            'map' => map_schema($ref, type_map(type_string(), type_integer())),
            'union' => union_schema($ref, type_union(type_string(), type_integer())),
            'html' => html_schema($ref),
            'htmlel' => html_element_schema($ref),
            'xml' => xml_schema($ref),
            'xmlel' => xml_element_schema($ref),
        ];
    }
}
