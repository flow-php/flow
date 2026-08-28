<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\ArrayGet;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_exists;
use function Flow\ETL\DSL\array_get;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayGetTest extends FlowTestCase
{
    public function test_constructor_rejects_a_wildcard_path(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('contains a wildcard');

        new ArrayGet(ref('array'), '*.id');
    }

    public function test_constructor_rejects_a_branch_path(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('contains a wildcard');

        new ArrayGet(ref('array'), '{a,b}');
    }

    public function test_array_access_for_not_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGet function failed to get value from array.');

        array_get(ref('integer_entry'), 'invalid_path')->eval(row(int_entry('integer_entry', 1)), flow_context());
        array_exists(ref('integer_entry'), 'invalid_path')->eval(row(int_entry('integer_entry', 1)), flow_context());
    }

    public function test_array_access_for_not_array_entry_strict_mode(): void
    {
        $context = flow_context();
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGet function failed to get value from array');

        array_get(ref('integer_entry'), 'invalid_path')->eval(row(int_entry('integer_entry', 1)), $context);
    }

    public function test_array_accessor_transformer(): void
    {
        $row = row(json_entry('array_entry', [
            'id' => 1,
            'status' => 'PENDING',
            'enabled' => true,
            'array' => ['foo' => 'bar'],
        ]));
        static::assertEquals('bar', array_get(ref('array_entry'), 'array.foo')->eval($row, flow_context()));
        static::assertTrue(array_exists(ref('array_entry'), 'array.foo')->eval($row, flow_context()));
    }

    public function test_array_accessor_transformer_with_invalid_and_without_strict_path(): void
    {
        $row = row(json_entry('array_entry', [
            'id' => 1,
            'status' => 'PENDING',
            'enabled' => true,
            'datetime' => new DateTimeImmutable('2020-01-01 00:00:00 UTC'),
            'array' => ['foo' => 'bar'],
        ]));
        static::assertNull(array_get(ref('array_entry'), '?invalid_path')->eval($row, flow_context()));
        static::assertTrue(array_exists(ref('array_entry'), '?invalid_path')->eval($row, flow_context()));
        static::assertFalse(array_exists(ref('array_entry'), 'invalid_path')->eval($row, flow_context()));
    }

    public function test_array_accessor_transformer_with_invalid_but_strict_path(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array ');

        array_get(ref('array_entry'), 'invalid_path')->eval(
            row(json_entry('array_entry', [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'datetime' => new DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                'array' => ['foo' => 'bar'],
            ])),
            flow_context(),
        );
    }

    public function test_returns_rejects_a_non_structure_input(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            array_get(ref('list'), 'field'),
            schema(list_schema('list', type_list(type_string()))),
        );

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage(
            'array_get() cannot describe the column it produces: the array operand declares "list<string>", which is not a structure.',
        );

        $resolved->returns();
    }

    public function test_returns_rejects_an_undeclared_path_segment(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            array_get(ref('data'), 'missing'),
            schema(structure_schema('data', type_structure(['field' => type_integer()]))),
        );

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('path segment "missing" is not declared by');

        $resolved->returns();
    }

    public function test_returns_resolves_the_element_type_for_a_declared_path(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            array_get(ref('data'), 'field'),
            schema(structure_schema('data', type_structure(['field' => type_integer()]))),
        );

        static::assertSame('integer', $resolved->returns()->toString());
    }
}
