<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_entry;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayValuesTest extends FlowTestCase
{
    public function test_array_values(): void
    {
        static::assertSame(
            [1, 2],
            ref('map')
                ->arrayValues()
                ->eval(
                    row(map_entry('map', ['a' => 1, 'b' => 2], type_map(type_string(), type_integer()))),
                    flow_context(),
                ),
        );
    }

    public function test_array_values_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        $context = flow_context(config());
        ref('map')->arrayValues()->eval(row(string_entry('map', 'test')), $context);
    }

    public function test_array_values_on_non_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        ref('map')->arrayValues()->eval(row(string_entry('map', 'test')), flow_context());
    }

    public function test_a_structure_operand_declares_the_unified_field_type(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->arrayValues(),
            schema(structure_schema('structure', type_structure(['a' => type_integer(), 'b' => type_integer()]))),
        );

        static::assertSame('list<integer>', $resolved->returns()->toString());
    }

    public function test_a_structure_whose_fields_do_not_unify_refuses_to_declare(): void
    {
        $this->expectException(SchemaNotDerivableException::class);

        (new ReferenceResolver())
            ->resolve(
                ref('structure')->arrayValues(),
                schema(structure_schema('structure', type_structure([
                    'a' => type_boolean(),
                    'b' => type_list(type_integer()),
                ]))),
            )
            ->returns();
    }
}
