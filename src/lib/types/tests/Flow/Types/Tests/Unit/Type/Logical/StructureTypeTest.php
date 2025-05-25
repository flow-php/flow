<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_datetime,
    type_float,
    type_from_array,
    type_integer,
    type_list,
    type_map,
    type_optional,
    type_string,
    type_structure};
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class StructureTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid structure with required fields' => [
            'value' => ['id' => 1, 'name' => 'b'],
            'exceptionClass' => null,
            'useOptionalName' => false,
        ];

        yield 'valid structure with optional field' => [
            'value' => ['id' => 1, 'name' => null],
            'exceptionClass' => null,
            'useOptionalName' => true,
        ];

        yield 'invalid string' => [
            'value' => 'string',
            'exceptionClass' => InvalidTypeException::class,
            'useOptionalName' => false,
        ];

        yield 'invalid UUID string' => [
            'value' => '49e952c8-80ec-4910-a1d6-a19bd46b163d',
            'exceptionClass' => InvalidTypeException::class,
            'useOptionalName' => false,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'exceptionClass' => InvalidTypeException::class,
            'useOptionalName' => false,
        ];

        yield 'invalid float' => [
            'value' => 124.25,
            'exceptionClass' => InvalidTypeException::class,
            'useOptionalName' => false,
        ];

        yield 'invalid array with different keys' => [
            'value' => ['a' => 'a', 'b' => 'b'],
            'exceptionClass' => InvalidTypeException::class,
            'useOptionalName' => false,
        ];

        yield 'invalid object' => [
            'value' => new \stdClass(),
            'exceptionClass' => InvalidTypeException::class,
            'useOptionalName' => false,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new \DateTimeZone('UTC'),
            'exceptionClass' => InvalidTypeException::class,
            'useOptionalName' => false,
        ];

        yield 'invalid structure with null required field' => [
            'value' => ['id' => null, 'name' => 'b'],
            'exceptionClass' => InvalidTypeException::class,
            'useOptionalName' => false,
        ];

        yield 'invalid structure with null required field 2' => [
            'value' => ['id' => 2, 'name' => null],
            'exceptionClass' => InvalidTypeException::class,
            'useOptionalName' => false,
        ];

        yield 'invalid structure with all null fields' => [
            'value' => ['id' => null, 'name' => null],
            'exceptionClass' => InvalidTypeException::class,
            'useOptionalName' => false,
        ];

        yield 'invalid structure with extra field' => [
            'value' => ['id' => 1, 'name' => null, 'active' => false],
            'exceptionClass' => InvalidTypeException::class,
            'useOptionalName' => false,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'array into structure' => [
            'structure' => type_structure([
                'name' => type_string(),
                'age' => type_integer(),
                'address' => type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ]),
            ]),
            'value' => [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => [
                    'street' => 'Polna',
                    'city' => 'Warsaw',
                ],
            ],
            'expected' => [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => [
                    'street' => 'Polna',
                    'city' => 'Warsaw',
                ],
            ],
            'exceptionClass' => null,
        ];

        yield 'structure with empty not nullable fields' => [
            'structure' => type_structure([
                'name' => type_string(),
                'age' => type_integer(),
                'address' => type_structure([
                    'street' => type_optional(type_string()),
                    'city' => type_optional(type_string()),
                ]),
            ]),
            'value' => [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => [],
            ],
            'expected' => [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => [
                    'street' => null,
                    'city' => null,
                ],
            ],
            'exceptionClass' => null,
        ];

        yield 'structure with missing nullable fields' => [
            'structure' => type_structure([
                'name' => type_string(),
                'age' => type_integer(),
                'address' => type_optional(type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ])),
            ]),
            'value' => [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
            ],
            'expected' => [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => null,
            ],
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid simple structure' => [
            'structure' => type_structure(['string' => type_string()]),
            'value' => ['string' => 'two'],
            'expected' => true,
        ];

        yield 'valid complex structure' => [
            'structure' => type_structure([
                'map' => type_map(type_integer(), type_map(type_string(), type_list(type_integer()))),
                'string' => type_string(),
                'float' => type_float(),
            ]),
            'value' => ['map' => [0 => ['one' => [1, 2]], 1 => ['two' => [3, 4]]], 'string' => 'c', 'float' => 1.5],
            'expected' => true,
        ];

        yield 'invalid indexed array' => [
            'structure' => type_structure(['int' => type_integer()]),
            'value' => [1, 2],
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null, bool $useOptionalName = false) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
        }

        if ($useOptionalName) {
            $result = type_structure(['id' => type_integer(), 'name' => type_optional(type_string())])->assert($value);
        } else {
            $result = type_structure(['id' => type_integer(), 'name' => type_string()])->assert($value);
        }

        if ($exceptionClass === null) {
            self::assertIsArray($result);
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast($structure, mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
        }

        $result = $structure->cast($value);

        if ($exceptionClass === null) {
            self::assertSame($expected, $result);
        }
    }

    public function test_elements() : void
    {
        self::assertEquals(
            $map = ['map' => type_map(type_string(), type_float())],
            type_structure($map)->elements()
        );
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid($structure, mixed $value, bool $expected) : void
    {
        self::assertSame($expected, $structure->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_structure([
            'string' => type_string(),
            'float' => type_float(),
            'map' => type_map(type_string(), type_list(type_datetime())),
        ]);
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        $struct = type_structure([
            'string' => type_string(),
            'float' => type_float(),
            'map' => type_map(type_string(), type_list(type_datetime())),
        ]);

        self::assertSame(
            'structure{string: string, float: float, map: map<string, list<datetime>>}',
            $struct->toString()
        );
    }
}
