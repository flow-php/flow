<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayReverseTest extends FlowTestCase
{
    public function test_array_reverse_array_entry(): void
    {
        static::assertSame([5, 3, 10, 4], ref('a')->arrayReverse()->eval(row(['a' => [4, 10, 3, 5]]), flow_context()));
    }

    public function test_array_reverse_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $context = flow_context(config());
        ref('a')->arrayReverse()->eval(row(['a' => 123]), $context);
    }

    public function test_array_reverse_non_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        ref('a')->arrayReverse()->eval(row(['a' => 123]), flow_context());
    }

    public function test_a_structure_operand_declares_reversed_fields(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->arrayReverse(),
            schema(structure_schema('structure', type_structure(['a' => type_integer(), 'b' => type_string()]))),
        );

        static::assertSame('structure{b: string, a: integer}', $resolved->returns()->toString());
    }

    public function test_optional_structure_fields_are_reversed_too(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->arrayReverse(),
            schema(structure_schema('structure', type_structure([
                'a' => type_integer(),
                'x' => structure_element('x', type_string(), optional: true),
                'y' => structure_element('y', type_string(), optional: true),
            ]))),
        );

        static::assertSame('structure{y?: string, x?: string, a: integer}', $resolved->returns()->toString());
    }

    public function test_reversal_crosses_the_required_and_optional_buckets(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->arrayReverse(),
            schema(structure_schema('structure', type_structure([
                'a' => type_integer(),
                'b' => type_integer(),
                'x' => structure_element('x', type_string(), optional: true),
            ]))),
        );

        static::assertSame('structure{x?: string, b: integer, a: integer}', $resolved->returns()->toString());
    }
}
