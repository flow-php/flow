<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class OnEachTest extends FlowTestCase
{
    public function test_a_throwing_element_is_not_swallowed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ref('array')
            ->onEach(ref('element')->upper())
            ->eval(row(['array' => ['a', 'b', ['nested' => 1], 'd']]), flow_context());
    }

    public function test_executing_function_on_each_value_from_array(): void
    {
        static::assertSame(
            ['1', '2', '3', '4', '5'],
            ref('array')
                ->onEach(ref('element')->cast(type_string()))
                ->eval(row(['array' => [1, 2, 3, 4, 5]]), flow_context()),
        );
    }

    public function test_executing_function_on_each_value_from_empty_array(): void
    {
        static::assertSame(
            [],
            ref('array')->onEach(ref('element')->cast(type_string()))->eval(row(['array' => []]), flow_context()),
        );
    }

    public function test_executing_function_on_each_value_with_preserving_keys(): void
    {
        static::assertSame(
            ['a' => '1', 'b' => '2', 'c' => '3', 'd' => '4', 'e' => '5'],
            ref('array')
                ->onEach(ref('element')->cast(type_string()), true)
                ->eval(row(['array' => ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5]]), flow_context()),
        );
    }

    public function test_executing_function_on_each_value_without_preserving_keys(): void
    {
        static::assertSame(
            ['1', '2', '3', '4', '5'],
            ref('array')
                ->onEach(ref('element')->cast(type_string()), false)
                ->eval(row(['array' => ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5]]), flow_context()),
        );
    }

    public function test_a_structure_operand_keeps_its_fields_typed_by_the_body(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->onEach(ref('element')->cast(type_string())),
            schema(structure_schema('structure', type_structure(['a' => type_integer(), 'b' => type_float()]))),
        );

        static::assertSame('structure{a: string, b: string}', $resolved->returns()->toString());
    }

    public function test_an_interleaved_structure_operand_keeps_every_position_and_flag(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->onEach(ref('element')->cast(type_string())),
            schema(structure_schema('structure', type_structure([
                'z' => type_integer(),
                'a' => structure_element('a', type_float(), optional: true),
                'b' => type_integer(),
            ]))),
        );

        static::assertSame('structure{z: string, a?: string, b: string}', $resolved->returns()->toString());
    }

    public function test_a_structure_operand_without_keys_declares_a_list_of_the_body_type(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->onEach(ref('element')->cast(type_string()), preserveKeys: false),
            schema(structure_schema('structure', type_structure(['a' => type_integer()]))),
        );

        static::assertSame('list<string>', $resolved->returns()->toString());
    }

    public function test_a_map_operand_with_preserved_keys_declares_a_map_of_the_body_type(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('map')->onEach(ref('element')->cast(type_string())),
            schema(map_schema('map', type_map(type_string(), type_integer()))),
        );

        static::assertSame('map<string, string>', $resolved->returns()->toString());
    }

    public function test_a_structure_with_non_unifying_fields_refuses_to_declare(): void
    {
        $this->expectException(SchemaNotDerivableException::class);

        (new ReferenceResolver())
            ->resolve(
                ref('structure')->onEach(ref('element')->cast(type_string())),
                schema(structure_schema('structure', type_structure([
                    'a' => type_boolean(),
                    'b' => type_list(type_integer()),
                ]))),
            )
            ->returns();
    }
}
