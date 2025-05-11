<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\{type_datetime, type_integer, type_list, type_map, type_structure};
use function Flow\ETL\DSL\{type_float, type_int, type_string};
use Flow\ETL\Exception\InvalidTypeException;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class StructureTypeTest extends FlowTestCase
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
                    'street' => type_string(true),
                    'city' => type_string(true),
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
                'address' => type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ], true),
            ], true)->cast(
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
            (type_structure($map))->elements()
        );
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_structure(['id' => type_int(), 'name' => type_string()])->assert($value);
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        self::assertIsArray(type_structure(['id' => type_int(), 'name' => type_string(true)])->assert($value));
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
            (type_structure(['string' => type_string()]))->isValid(['string' => 'two'])
        );
        self::assertTrue(
            (type_structure(['string' => type_string()], true))->isValid(null)
        );
        self::assertTrue(
            (
                type_structure([
                    'map' => type_map(type_integer(), type_map(type_string(), type_list(type_integer()))),
                    'string' => type_string(),
                    'float' => type_float(),
                ])
            )->isValid(['map' => [0 => ['one' => [1, 2]], 1 => ['two' => [3, 4]]], 'string' => 'c', 'float' => 1.5])
        );
        self::assertFalse(
            (type_structure(['int' => type_int()]))->isValid([1, 2])
        );
    }
}
