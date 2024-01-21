<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\structure_element;
use function Flow\ETL\DSL\structure_type;
use function Flow\ETL\DSL\type_integer;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\PHP\Type\Caster\StructureCastingHandler;
use PHPUnit\Framework\TestCase;

final class StructureCastingHandlerTest extends TestCase
{
    public function test_casting_array_into_structure() : void
    {
        $this->assertSame(
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
                Caster::default()
            )
        );
    }

    public function test_casting_structure_with_empty_not_nullable_fields() : void
    {
        $this->assertSame(
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
                Caster::default()
            )
        );
    }

    public function test_casting_structure_with_missing_nullable_fields() : void
    {
        $this->assertSame(
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
                Caster::default()
            )
        );
    }
}
