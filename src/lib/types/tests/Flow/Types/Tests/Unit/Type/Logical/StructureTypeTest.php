<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_boolean,
    type_datetime,
    type_float,
    type_from_array,
    type_integer,
    type_list,
    type_map,
    type_optional,
    type_string,
    type_structure};
use Flow\Types\Exception\{InvalidArgumentException, InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class StructureTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid structure with required fields' => [
            'value' => ['id' => 1, 'name' => 'b'],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => null,
        ];

        yield 'valid structure with optional field' => [
            'value' => ['id' => 1, 'name' => null],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_optional(type_string())]),
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'string',
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid UUID string' => [
            'value' => '49e952c8-80ec-4910-a1d6-a19bd46b163d',
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid float' => [
            'value' => 124.25,
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array with different keys' => [
            'value' => ['a' => 'a', 'b' => 'b'],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object' => [
            'value' => new \stdClass(),
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new \DateTimeZone('UTC'),
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid structure with null required field' => [
            'value' => ['id' => null, 'name' => 'b'],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid structure with null required field 2' => [
            'value' => ['id' => 2, 'name' => null],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid structure with all null fields' => [
            'value' => ['id' => null, 'name' => null],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid structure with extra field' => [
            'value' => ['id' => 1, 'name' => null, 'active' => false],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'valid structure with extra field when allow_extra is true' => [
            'value' => ['id' => 1, 'name' => 'test', 'active' => false],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()], [], true),
            'exceptionClass' => null,
        ];

        yield 'valid structure with multiple extra fields when allow_extra is true' => [
            'value' => ['id' => 1, 'name' => 'test', 'active' => false, 'created_at' => '2023-01-01'],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()], [], true),
            'exceptionClass' => null,
        ];

        yield 'invalid structure with missing required field even when allow_extra is true' => [
            'value' => ['name' => 'test', 'active' => false],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()], [], true),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'valid structure with optional elements present' => [
            'value' => ['id' => 1, 'name' => 'test', 'active' => false],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()], ['active' => type_boolean()]),
            'exceptionClass' => null,
        ];

        yield 'valid structure with optional elements missing' => [
            'value' => ['id' => 1, 'name' => 'test'],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()], ['active' => type_boolean()]),
            'exceptionClass' => null,
        ];

        yield 'valid structure with some optional elements present' => [
            'value' => ['id' => 1, 'name' => 'test', 'active' => false],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()], ['active' => type_boolean(), 'created_at' => type_string()]),
            'exceptionClass' => null,
        ];

        yield 'invalid structure with wrong type for optional element' => [
            'value' => ['id' => 1, 'name' => 'test', 'active' => 'invalid'],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()], ['active' => type_boolean()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid structure with unknown field when optional elements present and allow_extra false' => [
            'value' => ['id' => 1, 'name' => 'test', 'unknown' => 'value'],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()], ['active' => type_boolean()]),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'valid structure with unknown field when optional elements present and allow_extra true' => [
            'value' => ['id' => 1, 'name' => 'test', 'unknown' => 'value'],
            'structureType' => type_structure(['id' => type_integer(), 'name' => type_string()], ['active' => type_boolean()], true),
            'exceptionClass' => null,
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

        yield 'invalid structure with extra fields when allow_extra is false' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()]),
            'value' => ['id' => 1, 'name' => 'test', 'active' => true],
            'expected' => false,
        ];

        yield 'valid structure with extra fields when allow_extra is true' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()], [], true),
            'value' => ['id' => 1, 'name' => 'test', 'active' => true],
            'expected' => true,
        ];

        yield 'valid structure with multiple extra fields when allow_extra is true' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()], [], true),
            'value' => ['id' => 1, 'name' => 'test', 'active' => true, 'created_at' => '2023-01-01', 'updated_at' => '2023-01-02'],
            'expected' => true,
        ];

        yield 'invalid structure with missing required field when allow_extra is true' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()], [], true),
            'value' => ['name' => 'test', 'active' => true],
            'expected' => false,
        ];

        yield 'valid structure with only required fields when allow_extra is true' => [
            'structure' => type_structure(['id' => type_integer(), 'name' => type_string()], [], true),
            'value' => ['id' => 1, 'name' => 'test'],
            'expected' => true,
        ];

        yield 'valid structure with optional elements present' => [
            'structure' => type_structure(['id' => type_integer()], ['name' => type_string(), 'active' => type_boolean()]),
            'value' => ['id' => 1, 'name' => 'test', 'active' => true],
            'expected' => true,
        ];

        yield 'valid structure with some optional elements present' => [
            'structure' => type_structure(['id' => type_integer()], ['name' => type_string(), 'active' => type_boolean()]),
            'value' => ['id' => 1, 'name' => 'test'],
            'expected' => true,
        ];

        yield 'valid structure with no optional elements present' => [
            'structure' => type_structure(['id' => type_integer()], ['name' => type_string(), 'active' => type_boolean()]),
            'value' => ['id' => 1],
            'expected' => true,
        ];

        yield 'invalid structure with wrong type for optional element' => [
            'structure' => type_structure(['id' => type_integer()], ['name' => type_string()]),
            'value' => ['id' => 1, 'name' => 123],
            'expected' => false,
        ];

        yield 'invalid structure with extra field when optional elements present and allow_extra false' => [
            'structure' => type_structure(['id' => type_integer()], ['name' => type_string()]),
            'value' => ['id' => 1, 'name' => 'test', 'unknown' => 'value'],
            'expected' => false,
        ];

        yield 'valid structure with extra field when optional elements present and allow_extra true' => [
            'structure' => type_structure(['id' => type_integer()], ['name' => type_string()], true),
            'value' => ['id' => 1, 'name' => 'test', 'unknown' => 'value'],
            'expected' => true,
        ];
    }

    public function test_allows_extra_false_by_default() : void
    {
        $type = type_structure(['id' => type_integer()]);
        self::assertFalse($type->allowsExtra());
    }

    public function test_allows_extra_true_when_set() : void
    {
        $type = type_structure(['id' => type_integer()], [], true);
        self::assertTrue($type->allowsExtra());
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, $structureType, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $structureType->assert($value);
        } else {
            self::assertIsArray($structureType->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast($structure, mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $structure->cast($value);
        } else {
            self::assertSame($expected, $structure->cast($value));
        }
    }

    public function test_constructor_allows_empty_required_if_optional_provided() : void
    {
        $type = type_structure([], ['id' => type_integer()]);
        self::assertEmpty($type->elements());
        self::assertNotEmpty($type->optionalElements());
    }

    public function test_constructor_prevents_duplicate_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Element keys cannot be both required and optional: id');

        type_structure(['id' => type_integer()], ['id' => type_string()]);
    }

    public function test_constructor_requires_at_least_one_element() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Structure must receive at least one element (required or optional)');

        type_structure([], []);
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

    public function test_normalization_with_allow_extra() : void
    {
        $type = type_structure([
            'string' => type_string(),
            'float' => type_float(),
        ], [], true);
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
        self::assertTrue($recreated->allowsExtra());
    }

    public function test_normalization_with_optional_elements() : void
    {
        $type = type_structure(
            ['id' => type_integer()],
            ['name' => type_string(), 'active' => type_boolean()]
        );
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
        self::assertEquals(['name' => type_string(), 'active' => type_boolean()], $recreated->optionalElements());
    }

    public function test_optional_elements() : void
    {
        $optionalElements = ['name' => type_string(), 'active' => type_boolean()];
        $type = type_structure(['id' => type_integer()], $optionalElements);

        self::assertEquals($optionalElements, $type->optionalElements());
    }

    public function test_optional_elements_empty_by_default() : void
    {
        $type = type_structure(['id' => type_integer()]);
        self::assertEmpty($type->optionalElements());
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

    public function test_to_string_with_optional_elements() : void
    {
        $struct = type_structure(
            ['id' => type_integer(), 'name' => type_string()],
            ['active' => type_boolean(), 'score' => type_float()]
        );

        self::assertSame(
            'structure{id: integer, name: string, active?: boolean, score?: float}',
            $struct->toString()
        );
    }
}
