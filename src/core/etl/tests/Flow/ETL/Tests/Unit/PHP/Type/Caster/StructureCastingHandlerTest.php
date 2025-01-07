<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster;

use function Flow\ETL\DSL\{caster,
    caster_options,
    type_integer,
    type_string,
    type_structure};
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
                type_structure([
                    'name' => type_string(),
                    'age' => type_integer(),
                    'address' => type_structure([
                        'street' => type_string(),
                        'city' => type_string(),
                    ]),
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
                type_structure([
                    'name' => type_string(),
                    'age' => type_integer(),
                    'address' => type_structure([
                        'street' => type_string(true),
                        'city' => type_string(true),
                    ]),
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
                type_structure([
                    'name' => type_string(),
                    'age' => type_integer(),
                    'address' => type_structure([
                        'street' => type_string(),
                        'city' => type_string(),
                    ], true),
                ], true),
                caster(),
                caster_options()
            )
        );
    }
}
