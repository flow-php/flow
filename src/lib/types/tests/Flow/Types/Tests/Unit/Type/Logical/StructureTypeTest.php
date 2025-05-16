<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_datetime,
    type_float,
    type_integer,
    type_list,
    type_map,
    type_optional,
    type_string,
    type_structure};
use Flow\ETL\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class StructureTypeTest extends TestCase
{
    public static function invalid_assert_data_provider() : \Generator
    {
        yield ['string'];
        yield ['49e952c8-80ec-4910-a1d6-a19bd46b163d'];
        yield [false];
        yield [124.25];
        yield [['a' => 'a', 'b' => 'b']];
        yield [new \stdClass()];
        yield [new \DateTimeZone('UTC')];
        yield [['id' => null, 'name' => 'b']];
        yield [['id' => 1, 'name' => null]];
        yield [['id' => null, 'name' => null]];
        yield [['id' => 1, 'name' => null, 'active' => false]];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [['id' => 1, 'name' => 'b']];
        yield [['id' => 1, 'name' => null]];
    }

    public function test_casting_array_into_structure() : void
    {
        self::assertSame(
            [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => [
                    'street' => 'Polna',
                    'city' => 'Warsaw',
                ],
            ],
            type_structure([
                'name' => type_string(),
                'age' => type_integer(),
                'address' => type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ]),
            ])->cast(
                [
                    'name' => 'Norbert Orzechowicz',
                    'age' => 30,
                    'address' => [
                        'street' => 'Polna',
                        'city' => 'Warsaw',
                    ],
                ]
            )
        );
    }

    public function test_casting_structure_with_empty_not_nullable_fields() : void
    {
        self::assertSame(
            [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => [
                    'street' => null,
                    'city' => null,
                ],
            ],
            type_structure([
                'name' => type_string(),
                'age' => type_integer(),
                'address' => type_structure([
                    'street' => type_optional(type_string()),
                    'city' => type_optional(type_string()),
                ]),
            ])->cast(
                [
                    'name' => 'Norbert Orzechowicz',
                    'age' => 30,
                    'address' => [],
                ]
            )
        );
    }

    public function test_casting_structure_with_missing_nullable_fields() : void
    {
        self::assertSame(
            [
                'name' => 'Norbert Orzechowicz',
                'age' => 30,
                'address' => null,
            ],
            type_structure([
                'name' => type_string(),
                'age' => type_integer(),
                'address' => type_optional(type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ])),
            ])->cast(
                [
                    'name' => 'Norbert Orzechowicz',
                    'age' => 30,
                ],
            )
        );
    }

    public function test_elements() : void
    {
        self::assertEquals(
            $map = ['map' => type_map(type_string(), type_float())],
            type_structure($map)->elements()
        );
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_structure(['id' => type_integer(), 'name' => type_string()])->assert($value);
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        /** @phpstan-ignore-next-line */
        self::assertIsArray(type_structure(['id' => type_integer(), 'name' => type_optional(type_string())])->assert($value));
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

    public function test_valid() : void
    {
        self::assertTrue(
            /** @phpstan-ignore-next-line  */
            (type_structure(['string' => type_string()]))->isValid(['string' => 'two'])
        );
        self::assertTrue(
            /** @phpstan-ignore-next-line  */
            (
                type_structure([
                    'map' => type_map(type_integer(), type_map(type_string(), type_list(type_integer()))),
                    'string' => type_string(),
                    'float' => type_float(),
                ])
            )->isValid(['map' => [0 => ['one' => [1, 2]], 1 => ['two' => [3, 4]]], 'string' => 'c', 'float' => 1.5])
        );
        /** @phpstan-ignore-next-line  */
        self::assertFalse(
            /** @phpstan-ignore-next-line  */
            (type_structure(['int' => type_integer()]))->isValid([1, 2])
        );
    }
}
