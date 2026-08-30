<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\StructureSelect;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class StructureSelectTest extends FlowTestCase
{
    public function test_selecting_multiple_values_from_structure(): void
    {
        static::assertEquals(
            ['id' => 1, 'name' => 'test'],
            (new StructureSelect(ref('struct'), ref('id'), ref('name')))->eval(row(['struct' => [
                'id' => 1,
                'name' => 'test',
            ]]), flow_context()),
        );
    }

    public function test_selecting_single_value_from_structure(): void
    {
        static::assertEquals(
            ['id' => 1],
            (new StructureSelect(ref('struct'), 'id'))->eval(row(['struct' => [
                'id' => 1,
                'name' => 'test',
            ]]), flow_context()),
        );
    }

    public function test_selecting_single_value_from_structure_with_alias(): void
    {
        static::assertEquals(
            ['new_id' => 1],
            (new StructureSelect(ref('struct'), ref('id')->as('new_id')))->eval(row(['struct' => [
                'id' => 1,
                'name' => 'test',
            ]]), flow_context()),
        );
    }

    public function test_selecting_values_from_empty_structure(): void
    {
        static::assertEquals(
            ['new_id' => null],
            (new StructureSelect(ref('struct'), ref('id')->as('new_id')))->eval(row(['struct' => [
                'id' => null,
                'email' => 'email@email.com',
                'name' => null,
            ]]), flow_context()),
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
}
