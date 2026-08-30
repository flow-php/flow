<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function\Structure;

use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_ref;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class StructureSelectTest extends FlowTestCase
{
    public function test_selection_order_and_optional_flags_are_preserved(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            structure_ref('user')->select('b', 'a'),
            schema(structure_schema('user', type_structure([
                'a' => type_integer(),
                'b' => structure_element('b', type_string(), optional: true),
                'c' => type_integer(),
            ]))),
        );

        static::assertSame('?structure{b?: string, a: integer}', $resolved->returns()->toString());
    }

    public function test_selecting_a_numeric_element_name_by_its_string_form(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            structure_ref('user')->select('0'),
            schema(structure_schema('user', type_structure([0 => type_integer(), 'b' => type_string()]))),
        );

        static::assertSame('?structure{0: integer}', $resolved->returns()->toString());
    }

    public function test_structure_keep(): void
    {
        $rows = df()
            ->read(from_array([
                [
                    'user' => [
                        'id' => 1,
                        'name' => 'username',
                        'email' => 'user_email@email.com',
                        'tags' => [
                            'tag1',
                            'tag2',
                            'tag3',
                        ],
                    ],
                ],
            ]))
            ->withEntry('user', structure_ref('user')->select('id', 'email', 'tags'))
            ->fetch();

        static::assertEquals(
            [
                [
                    'user' => [
                        'id' => 1,
                        'email' => 'user_email@email.com',
                        'tags' => [
                            'tag1',
                            'tag2',
                            'tag3',
                        ],
                    ],
                ],
            ],
            $rows->toArray(),
        );
    }
}
