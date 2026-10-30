<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\StructureSelect;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class StructureSelectTest extends FlowTestCase
{
    public function test_selecting_multiple_values_from_structure(): void
    {
        static::assertEquals(
            ['id' => 1, 'name' => 'test'],
            (new FunctionContext(flow_context()))->eval(
                new StructureSelect(ref('struct'), ref('id'), ref('name')),
                ['struct' => [
                    'id' => 1,
                    'name' => 'test',
                ]],
                schema(structure_schema('struct', type_structure(['id' => type_integer(), 'name' => type_string()]))),
            ),
        );
    }

    public function test_selecting_single_value_from_structure(): void
    {
        static::assertEquals(
            ['id' => 1],
            (new FunctionContext(flow_context()))->eval(
                new StructureSelect(ref('struct'), 'id'),
                ['struct' => [
                    'id' => 1,
                    'name' => 'test',
                ]],
                schema(structure_schema('struct', type_structure(['id' => type_integer(), 'name' => type_string()]))),
            ),
        );
    }

    public function test_selecting_single_value_from_structure_with_alias(): void
    {
        static::assertEquals(
            ['new_id' => 1],
            (new FunctionContext(flow_context()))->eval(
                new StructureSelect(ref('struct'), ref('id')->as('new_id')),
                ['struct' => [
                    'id' => 1,
                    'name' => 'test',
                ]],
                schema(structure_schema('struct', type_structure(['id' => type_integer(), 'name' => type_string()]))),
            ),
        );
    }

    public function test_selecting_values_from_empty_structure(): void
    {
        static::assertEquals(
            ['new_id' => null],
            (new FunctionContext(flow_context()))->eval(
                new StructureSelect(ref('struct'), ref('id')->as('new_id')),
                ['struct' => [
                    'id' => null,
                    'email' => 'email@email.com',
                    'name' => null,
                ]],
                schema(structure_schema('struct', type_structure([
                    'id' => type_optional(type_integer()),
                    'email' => type_string(),
                    'name' => type_optional(type_string()),
                ]))),
            ),
        );
    }

    public function test_a_non_structure_operand_is_refused_at_bind(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            new StructureSelect(ref('list'), ref('id')),
            schema(list_schema(
                'list',
                type_list(type_structure([
                    'id' => type_integer(),
                    'name' => type_string(),
                ])),
            )),
        );

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('which is not a structure');

        $resolved->returns();
    }

    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();
        $function = new StructureSelect(ref('structure'), 'field');
        $rebuilt = DescribeRoundTrip::rebuilt($function);
        $rows = ScalarFunctionFixtures::rows(new PhpBackend());
        $schema = ScalarFunctionFixtures::schema();

        static::assertSame($context->exported($function, $rows, $schema), $context->exported($rebuilt, $rows, $schema));
    }
}
