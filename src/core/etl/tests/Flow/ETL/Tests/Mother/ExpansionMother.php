<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\Types\Type\Logical\StructureType;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ExpansionMother
{
    /**
     * A response page: {"rows":[{"keys":["term-1"],"clicks":1}, …]}.
     */
    public static function page(int $rows): string
    {
        return json_encode(['rows' => array_map(
            static fn(int $i): array => ['keys' => ['term-' . $i], 'clicks' => 1],
            range(1, $rows),
        )], JSON_THROW_ON_ERROR);
    }

    public static function pageRowType(): StructureType
    {
        return type_structure(['keys' => type_list(type_string()), 'clicks' => type_integer()]);
    }

    /**
     * @return list<string>
     */
    public static function terms(int $count): array
    {
        return array_map(static fn(int $i): string => 'term-' . $i, range(1, $count));
    }
}
