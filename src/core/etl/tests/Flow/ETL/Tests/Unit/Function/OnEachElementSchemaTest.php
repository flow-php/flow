<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\OnEachElementSchema;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class OnEachElementSchemaTest extends FlowTestCase
{
    public function test_a_list_declares_its_element_type(): void
    {
        static::assertEquals(
            schema(definition_from_type('element', type_integer())),
            (new OnEachElementSchema())->of(type_list(type_integer())),
        );
    }

    public function test_a_map_declares_its_value_type(): void
    {
        static::assertEquals(
            schema(definition_from_type('element', type_integer())),
            (new OnEachElementSchema())->of(type_map(type_string(), type_integer())),
        );
    }

    public function test_a_structure_declares_the_type_its_fields_unify_to(): void
    {
        static::assertSame(
            'integer',
            (new OnEachElementSchema())
                ->of(type_structure(['a' => type_integer(), 'b' => type_integer()]))
                ->get('element')
                ->type()
                ->toString(),
        );
    }

    public function test_an_optional_structure_field_still_unifies(): void
    {
        static::assertSame(
            'string',
            (new OnEachElementSchema())
                ->of(type_structure([
                    'a' => type_string(),
                    'b' => structure_element('b', type_string(), optional: true),
                ]))
                ->get('element')
                ->type()
                ->toString(),
        );
    }

    public function test_a_structure_whose_fields_do_not_unify_is_refused(): void
    {
        $this->expectException(SchemaNotDerivableException::class);

        (new OnEachElementSchema())->of(type_structure([
            'a' => type_boolean(),
            'b' => type_list(type_integer()),
        ]));
    }

    public function test_an_operand_with_no_element_type_is_refused(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('the array operand declares "array<mixed>", which has no element type');

        (new OnEachElementSchema())->of(type_array());
    }

    public function test_a_scalar_operand_is_refused(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('the array operand declares "float", which has no element type');

        (new OnEachElementSchema())->of(type_float());
    }
}
