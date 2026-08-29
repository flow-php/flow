<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayReverseTest extends FlowTestCase
{
    public function test_array_reverse_array_entry(): void
    {
        static::assertSame(
            [5, 3, 10, 4],
            ref('a')->arrayReverse()->eval(row(json_entry('a', [4, 10, 3, 5])), flow_context()),
        );
    }

    public function test_array_reverse_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $context = flow_context(config());
        ref('a')->arrayReverse()->eval(row(int_entry('a', 123)), $context);
    }

    public function test_array_reverse_non_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        ref('a')->arrayReverse()->eval(row(int_entry('a', 123)), flow_context());
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
            schema(structure_schema('structure', type_structure(['a' => type_integer()], [
                'x' => type_string(),
                'y' => type_string(),
            ]))),
        );

        static::assertSame('structure{a: integer, y?: string, x?: string}', $resolved->returns()->toString());
    }

    /**
     * Pins the bucket-scoped limit: required fields stay before optional ones, because
     * StructureType cannot represent an interleaved required/optional field order.
     */
    public function test_reversal_stays_within_the_required_and_optional_buckets(): void
    {
        $resolved = (new ReferenceResolver())->resolve(
            ref('structure')->arrayReverse(),
            schema(structure_schema('structure', type_structure(['a' => type_integer(), 'b' => type_integer()], [
                'x' => type_string(),
            ]))),
        );

        static::assertSame('structure{b: integer, a: integer, x?: string}', $resolved->returns()->toString());
    }
}
