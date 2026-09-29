<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\array_expand;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayExpandTest extends FlowTestCase
{
    public function test_expand_both(): void
    {
        static::assertSame(
            [
                ['a' => 1],
                ['b' => 2],
                ['c' => 3],
            ],
            (new FunctionContext(flow_context()))->eval(
                array_expand(ref('array'), ArrayExpand::BOTH),
                ['array' => [
                    'a' => 1,
                    'b' => 2,
                    'c' => 3,
                ]],
                schema(map_schema('array', type_map(type_string(), type_integer()))),
            ),
        );
    }

    public function test_expand_keys(): void
    {
        static::assertSame(
            ['a', 'b', 'c'],
            (new FunctionContext(flow_context()))->eval(
                array_expand(ref('array'), ArrayExpand::KEYS),
                ['array' => [
                    'a' => 1,
                    'b' => 2,
                    'c' => 3,
                ]],
                schema(map_schema('array', type_map(type_string(), type_integer()))),
            ),
        );
    }

    public function test_expand_values(): void
    {
        // an expand hands the step a list: the keys never reached the exploded column
        static::assertSame(
            [1, 2, 3],
            (new FunctionContext(flow_context()))->eval(
                array_expand(ref('array')),
                ['array' => [
                    'a' => 1,
                    'b' => 2,
                    'c' => 3,
                ]],
                schema(map_schema('array', type_map(type_string(), type_integer()))),
            ),
        );
    }

    public function test_for_not_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        (new FunctionContext(flow_context()))->eval(
            array_expand(ref('integer_entry')),
            ['integer_entry' => 1],
            schema(int_schema('integer_entry')),
        );
    }

    public function test_for_not_array_entry_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $context = flow_context(config());
        (new FunctionContext($context))->eval(
            array_expand(ref('integer_entry')),
            ['integer_entry' => 1],
            schema(int_schema('integer_entry')),
        );
    }

    public function test_values_over_a_structure_declare_the_unified_field_type(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            array_expand(ref('structure')),
            schema(structure_schema('structure', type_structure(['a' => type_integer(), 'b' => type_integer()]))),
        );

        static::assertSame('integer', $resolved->returns()->toString());
    }

    public function test_keys_declare_the_key_type(): void
    {
        $schema = schema(
            structure_schema('structure', type_structure(['a' => type_integer()])),
            list_schema('list', type_list(type_integer())),
        );

        static::assertSame(
            'string',
            (new ReferenceResolver())
                ->resolve(array_expand(ref('structure'), ArrayExpand::KEYS), $schema)
                ->returns()
                ->toString(),
        );
        static::assertSame(
            'integer',
            (new ReferenceResolver())
                ->resolve(array_expand(ref('list'), ArrayExpand::KEYS), $schema)
                ->returns()
                ->toString(),
        );
    }

    public function test_both_declares_a_one_pair_map(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            array_expand(ref('list'), ArrayExpand::BOTH),
            schema(list_schema('list', type_list(type_string()))),
        );

        static::assertSame('map<integer, string>', $resolved->returns()->toString());
    }

    public function test_a_map_operand_declares_its_value_and_key_types(): void
    {
        $schema = schema(map_schema('map', type_map(type_string(), type_integer())));

        static::assertSame(
            'integer',
            (new ReferenceResolver())
                ->resolve(array_expand(ref('map')), $schema)
                ->returns()
                ->toString(),
        );
        static::assertSame(
            'string',
            (new ReferenceResolver())
                ->resolve(array_expand(ref('map'), ArrayExpand::KEYS), $schema)
                ->returns()
                ->toString(),
        );
        static::assertSame(
            'map<string, integer>',
            (new ReferenceResolver())
                ->resolve(array_expand(ref('map'), ArrayExpand::BOTH), $schema)
                ->returns()
                ->toString(),
        );
    }

    public function test_keys_over_a_structure_with_non_unifying_fields_still_declare(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            array_expand(ref('structure'), ArrayExpand::KEYS),
            schema(structure_schema('structure', type_structure([
                'a' => type_boolean(),
                'b' => type_list(type_integer()),
            ]))),
        );

        static::assertSame('string', $resolved->returns()->toString());
    }

    public function test_values_over_a_structure_with_non_unifying_fields_refuse_to_declare(): void
    {
        $this->expectException(SchemaNotDerivableException::class);

        (new ReferenceResolver())
            ->resolve(
                array_expand(ref('structure')),
                schema(structure_schema('structure', type_structure([
                    'a' => type_boolean(),
                    'b' => type_list(type_integer()),
                ]))),
            )
            ->returns();
    }

    #[TestWith([ArrayExpand::VALUES])]
    #[TestWith([ArrayExpand::KEYS])]
    #[TestWith([ArrayExpand::BOTH])]
    public function test_a_null_array_expands_to_nothing(ArrayExpand $expand): void
    {
        static::assertSame(
            [],
            (new FunctionContext(flow_context()))->eval(
                array_expand(ref('list'), $expand),
                [
                    'list' => null,
                ],
                schema(str_schema('list', nullable: true)),
            ),
        );
    }
}
