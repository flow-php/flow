<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Row;
use Flow\ETL\Schema;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class ListColumnsMother
{
    public static function schema(): Schema
    {
        return schema(
            str_schema('id'),
            list_schema('tags', type_list(type_string())),
            list_schema('nums', type_list(type_integer())),
            list_schema('lists', type_list(type_list(type_string()))),
            list_schema('flags', type_list(type_boolean())),
        );
    }

    /**
     * @param array<string, mixed> $override
     */
    public static function row(array $override = []): Row
    {
        return row([
            'id' => 'a',
            'tags' => ['x', 'y'],
            'nums' => [1, 2, 3],
            'lists' => [['q', 'r'], ['s']],
            'flags' => [true, false],
            ...$override,
        ]);
    }

    public static function tagsSchema(): Schema
    {
        return schema(str_schema('id'), list_schema('tags', type_list(type_string())));
    }

    public static function numberAndFlagsSchema(): Schema
    {
        return schema(int_schema('n'), list_schema('flags', type_list(type_boolean())));
    }
}
