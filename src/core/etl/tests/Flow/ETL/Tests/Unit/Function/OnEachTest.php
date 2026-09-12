<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\OnEach;
use Flow\ETL\Function\OnEachElementSchema;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\Double\CountingReturnsFunction;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function serialize;
use function substr_count;

final class OnEachTest extends FlowTestCase
{
    public function test_a_throwing_body_is_not_swallowed(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer"');

        (new ReferenceResolver())
            ->resolve(
                ref('array')->onEach(ref('element')->upper()),
                schema(list_schema('array', type_list(type_integer()))),
            )
            ->eval(row(['array' => [1, 2, 3]]), flow_context());
    }

    public function test_an_element_the_declared_type_refuses_is_not_swallowed(): void
    {
        $this->expectException(SchemaMismatchException::class);

        (new ReferenceResolver())
            ->resolve(
                ref('array')->onEach(ref('element')->upper()),
                schema(list_schema('array', type_list(type_string()))),
            )
            ->eval(row(['array' => ['a', null]]), flow_context());
    }

    public function test_executing_function_on_each_value_from_array(): void
    {
        static::assertSame(
            ['1', '2', '3', '4', '5'],
            (new ReferenceResolver())
                ->resolve(
                    ref('array')->onEach(ref('element')->cast(type_string())),
                    schema(list_schema('array', type_list(type_integer()))),
                )
                ->eval(row(['array' => [1, 2, 3, 4, 5]]), flow_context()),
        );
    }

    public function test_executing_function_on_each_value_from_empty_array(): void
    {
        static::assertSame(
            [],
            (new ReferenceResolver())
                ->resolve(
                    ref('array')->onEach(ref('element')->cast(type_string())),
                    schema(list_schema('array', type_list(type_integer()))),
                )
                ->eval(row(['array' => []]), flow_context()),
        );
    }

    public function test_executing_function_on_each_value_with_preserving_keys(): void
    {
        static::assertSame(
            ['a' => '1', 'b' => '2', 'c' => '3', 'd' => '4', 'e' => '5'],
            (new ReferenceResolver())
                ->resolve(
                    ref('array')->onEach(ref('element')->cast(type_string()), true),
                    schema(map_schema('array', type_map(type_string(), type_integer()))),
                )
                ->eval(row(['array' => ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5]]), flow_context()),
        );
    }

    public function test_executing_function_on_each_value_without_preserving_keys(): void
    {
        static::assertSame(
            ['1', '2', '3', '4', '5'],
            (new ReferenceResolver())
                ->resolve(
                    ref('array')->onEach(ref('element')->cast(type_string()), false),
                    schema(map_schema('array', type_map(type_string(), type_integer()))),
                )
                ->eval(row(['array' => ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5]]), flow_context()),
        );
    }

    public function test_on_each_over_an_untyped_array_operand_is_refused(): void
    {
        // an ArrayType column is declared by a JsonDefinition, so this is what an untyped array
        // operand reaches on_each() as - either way it carries no element type
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('the array operand declares "json", which has no element type');

        (new ReferenceResolver())
            ->resolve(
                ref('array')->onEach(ref('element')->cast(type_string())),
                schema(definition_from_type('array', type_array())),
            )
            ->eval(row(['array' => [1, 2]]), flow_context());
    }

    public function test_the_element_schema_is_built_once_per_eval_not_once_per_element(): void
    {
        $operand = new CountingReturnsFunction(lit([1, 2, 3, 4, 5]), type_list(type_integer()));

        (new OnEach($operand, ref('element')->cast(type_string())))->eval(row([]), flow_context());

        static::assertSame(1, $operand->returnsCalls);
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

    #[DataProvider('bodies_holding_an_expand')]
    public function test_an_expand_in_the_body_is_refused_when_the_expression_is_built(ScalarFunction $body): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'onEach() cannot contain array_expand(), it turns one row into many rows. Expand with withEntry() first, then use the new column.',
        );

        ref('lists')->onEach($body);
    }

    /**
     * @return Generator<string, array{ScalarFunction}>
     */
    public static function bodies_holding_an_expand(): Generator
    {
        yield 'at the root' => [ref('element')->expand()];
        yield 'nested' => [ref('element')->expand()->equals(lit('x'))];
    }

    public function test_a_null_array_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('OnEach requires non-null array');

        ref('tags')->onEach(ref('element'))->eval(row(['tags' => null]), flow_context());
    }

    public function test_with_children_keeps_the_element_schema(): void
    {
        $elementSchema = new OnEachElementSchema();
        $rebuilt = (new OnEach(ref('a'), ref('element'), true, $elementSchema))->withChildren([ref('b')]);

        // two stateless instances are equal, but serialize() names an instance it already wrote only by back-reference
        static::assertSame(1, substr_count(serialize([$elementSchema, $rebuilt]), 'OnEachElementSchema'));
    }
}
