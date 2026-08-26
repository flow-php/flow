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
use function Flow\Types\DSL\type_empty_array;
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

        yield 'empty array excluded from the value type count' => [
            [
                type_integer(),
            ],
            [
                type_empty_array(),
                type_list(type_integer()),
            ],
            true,
            true,
        ];

        yield 'only empty arrays is not a list' => [
            [
                type_integer(),
            ],
            [
                type_empty_array(),
            ],
            true,
            false,
        ];

        yield 'heterogeneous arrays still excluded from the value type count' => [
            [
                type_integer(),
            ],
            [
                type_array(),
                type_structure(['id' => type_integer()]),
            ],
            true,
            true,
        ];

        yield 'scalar with an empty array is not a list' => [
            [
                type_integer(),
            ],
            [
                type_string(),
                type_empty_array(),
            ],
            true,
            false,
        ];

        yield 'scalar with a heterogeneous array is not a list' => [
            [
                type_integer(),
            ],
            [
                type_string(),
                type_array(),
            ],
            true,
            false,
        ];
    }

    public static function provide_map_data(): Generator
    {
        yield 'integer key, string value' => [
            [
                type_integer(),
            ],
            [
                type_string(),
            ],
            false,
            true,
        ];

        yield 'integer key, mixed values' => [
            [
                type_integer(),
            ],
            [
                type_string(),
                type_integer(),
            ],
            false,
            true,
        ];

        yield 'string string' => [
            [
                type_string(),
            ],
            [
                type_string(),
            ],
            false,
            false,
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

        yield 'integer key with only empty array values is not a map' => [
            [
                type_integer(),
            ],
            [
                type_empty_array(),
            ],
            false,
            false,
        ];

        yield 'scalar with an empty array is not a map' => [
            [
                type_integer(),
            ],
            [
                type_string(),
                type_empty_array(),
            ],
            false,
            false,
        ];

        yield 'list values with an empty array are still a map' => [
            [
                type_integer(),
            ],
            [
                type_list(type_integer()),
                type_empty_array(),
            ],
            false,
            true,
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

        yield 'homogeneous string keys' => [
            [
                type_string(),
            ],
            [
                type_string(),
            ],
            false,
            true,
        ];

        yield 'integer keys are never structural' => [
            [
                type_integer(),
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

        yield 'string keys, of maps string with string' => [
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
            true,
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
            true,
        ];

        yield 'string keys with empty array values' => [
            [
                type_string(),
            ],
            [
                type_empty_array(),
            ],
            false,
            true,
        ];
    }

    public static function provide_value_type_data(): Generator
    {
        yield 'empty array before a list keeps the list type' => [
            [
                type_empty_array(),
                type_list(type_integer()),
            ],
            'list<integer>',
        ];

        yield 'empty array after a list keeps the list type' => [
            [
                type_list(type_integer()),
                type_empty_array(),
            ],
            'list<integer>',
        ];

        yield 'null with an empty array becomes an optional list' => [
            [
                type_null(),
                type_empty_array(),
                type_list(type_integer()),
            ],
            '?list<integer>',
        ];

        yield 'only an empty array degrades to array<mixed>' => [
            [
                type_empty_array(),
            ],
            'array<mixed>',
        ];

        yield 'empty array with a structure degrades to array<mixed>' => [
            [
                type_structure(['id' => type_integer()]),
                type_empty_array(),
            ],
            'array<mixed>',
        ];

        yield 'heterogeneous array with a list degrades to array<mixed>' => [
            [
                type_array(),
                type_list(type_integer()),
            ],
            'array<mixed>',
        ];

        yield 'only null stays null' => [
            [
                type_null(),
            ],
            'null',
        ];

        yield 'null with a string becomes an optional string' => [
            [
                type_null(),
                type_string(),
            ],
            '?string',
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

    public function test_nested_empty_array_unifies_element_wise(): void
    {
        static::assertSame(
            'list<?string>',
            (new ArrayContentDetector(
                new Types(type_integer()),
                new Types(type_list(type_string()), type_list(type_null())),
                true,
            ))
                ->valueType()
                ->toString(),
        );

        static::assertSame(
            'list<null>',
            (new ArrayContentDetector(new Types(type_integer()), new Types(type_list(type_null())), true))
                ->valueType()
                ->toString(),
        );
    }

    public function test_value_types_without_a_common_type_widen_to_the_floor_element(): void
    {
        $detector = new ArrayContentDetector(new Types(type_integer()), new Types(type_integer(), type_string()), true);

        static::assertTrue($detector->isList());
        static::assertSame('string', $detector->valueType()->toString());
    }

    /**
     * @param array<Type<mixed>> $values
     */
    #[DataProvider('provide_value_type_data')]
    public function test_value_type(array $values, string $expected): void
    {
        static::assertSame(
            $expected,
            (new ArrayContentDetector(new Types(type_integer()), new Types(...$values), true))->valueType()->toString(),
        );
    }
}
