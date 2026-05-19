<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Type;
use Flow\Types\Type\ArrayContentDetector;
use Flow\Types\Type\Types;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayContentDetectorTest extends TestCase
{
    public static function provide_list_data(): Generator
    {
        yield 'simple list' => [
            [
                type_integer(),
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
                type_integer(),
            ],
            [
                type_structure([
                    'id' => type_integer(),
                    'name' => type_string(),
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

    public static function provide_map_data(): Generator
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
                type_integer(),
            ],
            [
                type_structure([
                    'id' => type_integer(),
                    'name' => type_string(),
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

    public static function provide_structure_data(): Generator
    {
        yield 'simple list' => [
            [
                type_integer(),
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
                type_integer(),
            ],
            [
                type_structure([
                    'id' => type_integer(),
                    'name' => type_string(),
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
                type_array(),
                type_array(),
                type_array(),
            ],
            false,
            false,
        ];
    }

    /**
     * @param array<Type<mixed>> $keys
     * @param array<Type<mixed>> $values
     */
    #[DataProvider('provide_list_data')]
    public function test_list_data(array $keys, array $values, bool $isList, bool $expected): void
    {
        static::assertSame(
            $expected,
            (new ArrayContentDetector(new Types(...$keys), new Types(...$values), $isList))->isList(),
        );
    }

    /**
     * @param array<Type<mixed>> $keys
     * @param array<Type<mixed>> $values
     */
    #[DataProvider('provide_map_data')]
    public function test_map_data(array $keys, array $values, bool $isList, bool $expected): void
    {
        static::assertSame(
            $expected,
            (new ArrayContentDetector(new Types(...$keys), new Types(...$values), $isList))->isMap(),
        );
    }

    /**
     * @param array<Type<mixed>> $keys
     * @param array<Type<mixed>> $values
     */
    #[DataProvider('provide_structure_data')]
    public function test_structure_data(array $keys, array $values, bool $isList, bool $expected): void
    {
        static::assertSame(
            $expected,
            (new ArrayContentDetector(new Types(...$keys), new Types(...$values), $isList))->isStructure(),
        );
    }
}
