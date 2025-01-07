<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type;

use function Flow\ETL\DSL\{structure_element, type_integer, type_list, type_map, type_structure};
use function Flow\ETL\DSL\{type_int, type_null, type_string};
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\{ArrayContentDetector, Types};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class ArrayContentDetectorTest extends FlowTestCase
{
    public static function provide_list_data() : \Generator
    {
        yield 'simple list' => [
            [
                type_int(),
            ],
            [
                type_string(),
            ],
            true,
            true,
        ];

        yield 'simple map' => [
            [
                type_string(),
            ],
            [
                type_string(),
            ],
            false,
            false,
        ];

        yield 'simple structure' => [
            [
                type_string(),
            ],
            [
                type_string(),
                type_map(type_string(), type_string()),
                type_list(type_integer()),
            ],
            false,
            false,
        ];

        yield 'list of unique same structures' => [
            [
                type_int(),
            ],
            [
                type_structure([
                    structure_element('id', type_int()),
                    structure_element('name', type_string()),
                ]),
            ],
            true,
            true,
        ];

        yield 'map with string key, of maps string with string' => [
            [
                type_string(),
            ],
            [
                type_map(type_string(), type_map(type_string(), type_string())),
            ],
            false,
            false,
        ];

        yield 'array of nulls' => [
            [
                type_string(),
            ],
            [
                type_null(),
                type_null(),
                type_null(),
            ],
            false,
            false,
        ];
    }

    public static function provide_map_data() : \Generator
    {
        yield 'string string' => [
            [
                type_string(),
            ],
            [
                type_string(),
            ],
            false,
            true,
        ];

        yield 'string structure{map<string,string>,list<int>}' => [
            [
                type_string(),
            ],
            [
                type_string(),
                type_map(type_string(), type_string()),
                type_list(type_integer()),
            ],
            false,
            false,
        ];

        yield 'list of unique same structures' => [
            [
                type_int(),
            ],
            [
                type_structure([
                    structure_element('id', type_int()),
                    structure_element('name', type_string()),
                ]),
            ],
            true,
            false,
        ];

        yield 'map with string key, of maps string with string' => [
            [
                type_string(),
            ],
            [
                type_map(type_string(), type_map(type_string(), type_string())),
            ],
            false,
            true,
        ];

        yield 'array of nulls' => [
            [
                type_string(),
            ],
            [
                type_null(),
                type_null(),
                type_null(),
            ],
            false,
            false,
        ];
    }

    public static function provide_structure_data() : \Generator
    {
        yield 'simple list' => [
            [
                type_int(),
            ],
            [
                type_string(),
            ],
            false,
            false,
        ];

        yield 'simple map' => [
            [
                type_string(),
            ],
            [
                type_string(),
            ],
            false,
            false,
        ];

        yield 'simple structure' => [
            [
                type_string(),
            ],
            [
                type_string(),
                type_map(type_string(), type_string()),
                type_list(type_integer()),
            ],
            true,
            true,
        ];

        yield 'list of unique same structures' => [
            [
                type_int(),
            ],
            [
                type_structure([
                    structure_element('id', type_int()),
                    structure_element('name', type_string()),
                ]),
            ],
            false,
            false,
        ];

        yield 'map with string key, of maps string with string' => [
            [
                type_string(),
            ],
            [
                type_map(type_string(), type_map(type_string(), type_string())),
            ],
            false,
            false,
        ];

        yield 'array of nulls' => [
            [
                type_string(),
            ],
            [
                type_null(),
                type_null(),
                type_null(),
            ],
            false,
            false,
        ];

        yield 'array of empty arrays' => [
            [
                type_string(),
            ],
            [
                ArrayType::empty(),
                ArrayType::empty(),
                ArrayType::empty(),
            ],
            false,
            false,
        ];
    }

    #[DataProvider('provide_list_data')]
    public function test_list_data(array $keys, array $values, bool $isList, bool $expected) : void
    {
        self::assertSame(
            $expected,
            (new ArrayContentDetector(new Types(...$keys), new Types(...$values), $isList))->isList()
        );
    }

    #[DataProvider('provide_map_data')]
    public function test_map_data(array $keys, array $values, bool $isList, bool $expected) : void
    {
        self::assertSame(
            $expected,
            (new ArrayContentDetector(new Types(...$keys), new Types(...$values), $isList))->isMap()
        );
    }

    #[DataProvider('provide_structure_data')]
    public function test_structure_data(array $keys, array $values, bool $isList, bool $expected) : void
    {
        self::assertSame(
            $expected,
            (new ArrayContentDetector(new Types(...$keys), new Types(...$values), $isList))->isStructure()
        );
    }
}
