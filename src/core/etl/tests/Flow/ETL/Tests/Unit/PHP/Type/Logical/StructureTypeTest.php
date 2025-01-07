<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\{type_boolean, type_float, type_int, type_string};
use function Flow\ETL\DSL\{type_datetime, type_integer, type_list, type_map, type_structure};
use Flow\ETL\Tests\FlowTestCase;

final class StructureTypeTest extends FlowTestCase
{
    public function test_elements() : void
    {
        self::assertEquals(
            $map = ['map' => type_map(type_string(), type_float())],
            (type_structure($map))->elements()
        );
    }

    public function test_equals() : void
    {
        self::assertTrue(
            (type_structure(['map' => type_map(type_string(), type_float())])
                ->isEqual(type_structure(['map' => type_map(type_string(), type_float())])))
        );
        self::assertFalse(
            (type_structure(['string' => type_string(), 'bool' => type_boolean()])
                ->isEqual(type_list(type_integer())))
        );
        self::assertFalse(
            (type_structure(['string' => type_string(), 'bool' => type_boolean()]))
                ->isEqual(type_structure(['bool' => type_boolean(), 'integer' => type_string()]))
        );
        self::assertTrue(
            type_structure([
                'string' => type_string(),
                'bool' => type_boolean(),
            ])
            ->isEqual(
                type_structure([
                    'string' => type_string(),
                    'bool' => type_boolean(),
                ])
            )
        );
        self::assertFalse(
            type_structure([
                'string' => type_string(),
                'bool' => type_boolean(),
            ])
                ->isEqual(
                    type_structure([
                        'string' => type_string(),
                        'bool' => type_boolean(true),
                    ])
                )
        );
    }

    public function test_merging_different_left_structure() : void
    {
        self::assertEquals(
            type_structure([
                'string' => type_string(true),
                'float' => type_float(),
                'map' => type_map(type_string(), type_list(type_list(type_datetime())), true),
            ]),
            type_structure([
                'string' => type_string(),
                'float' => type_float(),
            ])->merge(type_structure([
                'float' => type_float(),
                'map' => type_map(type_string(), type_list(type_list(type_datetime()))),
            ]))
        );
    }

    public function test_merging_different_right_structure() : void
    {
        self::assertEquals(
            type_structure([
                'string' => type_string(true),
                'float' => type_float(true),
                'map' => type_map(type_string(), type_list(type_list(type_datetime())), true),
            ]),
            type_structure([
                'string' => type_string(),
                'float' => type_float(),
            ])->merge(type_structure([
                'map' => type_map(type_string(), type_list(type_list(type_datetime()))),
            ]))
        );
    }

    public function test_merging_nested_structures() : void
    {
        self::assertEquals(
            type_structure([
                'string' => type_string(true),
                'float' => type_float(true),
                'structure' => type_structure(
                    [
                        'id' => type_string(),
                        'name' => type_float(),
                    ],
                    nullable: true
                ),
            ]),
            type_structure([
                'string' => type_string(),
                'float' => type_float(),
            ])->merge(
                type_structure([
                    'structure' => type_structure(
                        [
                            'id' => type_string(),
                            'name' => type_float(),
                        ],
                    ),
                ])
            )
        );
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
