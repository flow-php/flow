<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster,
    caster_options,
    struct_type,
    structure_element,
    structure_type,
    type_integer,
    type_string};
use Flow\ETL\PHP\Type\Caster\StructureCastingHandler;
use Flow\ETL\Tests\FlowTestCase;

final class StructureCastingHandlerTest extends FlowTestCase
{
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
            (new StructureCastingHandler())->value(
                [
                    'name' => 'Norbert Orzechowicz',
                    'age' => 30,
                    'address' => [
                        'street' => 'Polna',
                        'city' => 'Warsaw',
                    ],
                ],
                struct_type([
                    structure_element('name', type_string()),
                    structure_element('age', type_integer()),
                    structure_element(
                        'address',
                        structure_type([
                            structure_element('street', type_string()),
                            structure_element('city', type_string()),
                        ])
                    ),
                ]),
                caster(),
                caster_options()
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
            (new StructureCastingHandler())->value(
                [
                    'name' => 'Norbert Orzechowicz',
                    'age' => 30,
                    'address' => [],
                ],
                struct_type([
                    structure_element('name', type_string()),
                    structure_element('age', type_integer()),
                    structure_element(
                        'address',
                        structure_type([
                            structure_element('street', type_string(true)),
                            structure_element('city', type_string(true)),
                        ])
                    ),
                ]),
                caster(),
                caster_options()
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
            (new StructureCastingHandler())->value(
                [
                    'name' => 'Norbert Orzechowicz',
                    'age' => 30,
                ],
                struct_type([
                    structure_element('name', type_string()),
                    structure_element('age', type_integer()),
                    structure_element(
                        'address',
                        structure_type([
                            structure_element('street', type_string()),
                            structure_element('city', type_string()),
                        ], true)
                    ),
                ], true),
                caster(),
                caster_options()
            )
        );
    }
}
