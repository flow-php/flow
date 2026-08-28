<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_entry;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayKeysTest extends FlowTestCase
{
    public function test_a_structure_operand_declares_a_list_of_string_keys(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->arrayKeys(),
            schema(structure_schema('structure', type_structure(['field' => type_integer()]))),
        );

        static::assertSame('list<string>', $resolved->returns()->toString());
    }

    public function test_a_list_operand_declares_a_list_of_integer_keys(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            ref('list')->arrayKeys(),
            schema(list_schema('list', type_list(type_string()))),
        );

        static::assertSame('list<integer>', $resolved->returns()->toString());
    }

    public function test_an_operand_without_a_key_type_is_refused_at_bind(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(ref('json')->arrayKeys(), schema(json_schema('json')));

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('which has no key type');

        $resolved->returns();
    }

    public function test_array_keys(): void
    {
        static::assertSame(
            ['a', 'b'],
            ref('map')
                ->arrayKeys()
                ->eval(
                    row(map_entry('map', ['a' => 1, 'b' => 2], type_map(type_string(), type_integer()))),
                    flow_context(),
                ),
        );
    }

    public function test_array_keys_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        $context = flow_context(config());
        ref('map')->arrayKeys()->eval(row(string_entry('map', 'test')), $context);
    }

    public function test_array_keys_on_non_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        ref('map')->arrayKeys()->eval(row(string_entry('map', 'test')), flow_context());
    }
}
