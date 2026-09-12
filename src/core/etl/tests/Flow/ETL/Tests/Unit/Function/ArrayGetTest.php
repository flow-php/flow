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
use Flow\Types\Type\Logical\StructureType;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_exists;
use function Flow\ETL\DSL\array_get;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_get;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayGetTest extends FlowTestCase
{
    /**
     * @return array<string, array{StructureType<array<array-key, mixed>>, string, string}>
     */
    public static function path_grammar_types(): array
    {
        return [
            'nullsafe step on an optional element' => [
                type_structure(['x' => structure_element('x', type_list(type_integer()), optional: true)]),
                '?x',
                '?list<integer>',
            ],
            'plain step on an optional element' => [
                type_structure(['x' => structure_element('x', type_list(type_integer()), optional: true)]),
                'x',
                'list<integer>',
            ],
            'nullsafe step on a required element' => [
                type_structure(['x' => type_list(type_integer())]),
                '?x',
                'list<integer>',
            ],
            'nested nullsafe step' => [
                type_structure(['a' => type_structure(['b' => structure_element('b', type_string(), optional: true)])]),
                'a.?b',
                '?string',
            ],
            'nullsafe step on an optional parent' => [
                type_structure(['a' => structure_element('a', type_structure(['b' => type_string()]), optional: true)]),
                '?a.b',
                '?string',
            ],
            'nullable leaf stays nullable' => [
                type_structure(['x' => structure_element('x', type_optional(type_string()), optional: true)]),
                '?x',
                '?string',
            ],
            'nullsafe numeric step' => [
                type_structure([0 => structure_element(0, type_string(), optional: true), 'k' => type_string()]),
                '?0',
                '?string',
            ],
            'escaped dot' => [type_structure(['a.b' => type_string()]), 'a\\.b', 'string'],
            'escaped wildcard' => [type_structure(['*' => type_string()]), '\\*', 'string'],
            'escaped braces' => [type_structure(['{a}' => type_string()]), '\\{a\\}', 'string'],
            'star inside a key' => [type_structure(['a*b' => type_string()]), 'a*b', 'string'],
            'nullsafe step on an optional mixed element' => [
                type_structure(['x' => structure_element('x', type_mixed(), optional: true)]),
                '?x',
                'mixed',
            ],
        ];
    }

    public function test_constructor_rejects_a_wildcard_path(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('selects more than one value');

        new ArrayGet(ref('array'), '*.id');
    }

    public function test_constructor_rejects_a_branch_path(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('selects more than one value');

        new ArrayGet(ref('array'), '{a,b}');
    }

    public function test_array_access_for_not_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGet function failed to get value from array.');

        array_get(ref('integer_entry'), 'invalid_path')->eval(row(['integer_entry' => 1]), flow_context());
        array_exists(ref('integer_entry'), 'invalid_path')->eval(row(['integer_entry' => 1]), flow_context());
    }

    public function test_array_access_for_not_array_entry_strict_mode(): void
    {
        $context = flow_context();
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGet function failed to get value from array');

        array_get(ref('integer_entry'), 'invalid_path')->eval(row(['integer_entry' => 1]), $context);
    }

    public function test_array_accessor_transformer(): void
    {
        $row = row([
            'array_entry' => [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['foo' => 'bar'],
            ],
        ]);
        static::assertEquals('bar', array_get(ref('array_entry'), 'array.foo')->eval($row, flow_context()));
        static::assertTrue(array_exists(ref('array_entry'), 'array.foo')->eval($row, flow_context()));
    }

    public function test_array_accessor_transformer_with_invalid_and_without_strict_path(): void
    {
        $row = row([
            'array_entry' => [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'datetime' => new DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                'array' => ['foo' => 'bar'],
            ],
        ]);
        static::assertNull(array_get(ref('array_entry'), '?invalid_path')->eval($row, flow_context()));
        static::assertTrue(array_exists(ref('array_entry'), '?invalid_path')->eval($row, flow_context()));
        static::assertFalse(array_exists(ref('array_entry'), 'invalid_path')->eval($row, flow_context()));
    }

    public function test_array_accessor_transformer_with_invalid_but_strict_path(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array ');

        array_get(ref('array_entry'), 'invalid_path')->eval(row([
            'array_entry' => [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'datetime' => new DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                'array' => ['foo' => 'bar'],
            ],
        ]), flow_context());
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

    public function test_returns_resolves_a_numeric_path_segment_to_an_integer_named_element(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            array_get(ref('data'), '0'),
            schema(structure_schema('data', type_structure([0 => type_integer(), 'b' => type_string()]))),
        );

        static::assertSame('integer', $resolved->returns()->toString());
    }

    /**
     * @param StructureType<array<array-key, mixed>> $structure
     */
    #[DataProvider('path_grammar_types')]
    public function test_returns_follows_the_path_grammar(
        StructureType $structure,
        string $path,
        string $expected,
    ): void {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            array_get(ref('data'), $path),
            schema(structure_schema('data', $structure)),
        );

        static::assertSame($expected, $resolved->returns()->toString());
    }

    public function test_returns_names_the_key_of_an_undeclared_nullsafe_segment(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            array_get(ref('data'), '?missing'),
            schema(structure_schema('data', type_structure(['field' => type_integer()]))),
        );

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('path segment "missing" is not declared by');

        $resolved->returns();
    }

    public function test_constructor_rejects_a_nullsafe_wildcard_path(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('selects more than one value');

        new ArrayGet(ref('array'), '?*.id');
    }

    public function test_constructor_rejects_an_invalid_path(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayGet path "x.{a" is not a valid path.');

        new ArrayGet(ref('array'), 'x.{a');
    }

    public function test_structure_get_is_an_alias_of_array_get(): void
    {
        static::assertEquals(array_get(ref('data'), 'a.?b'), structure_get(ref('data'), 'a.?b'));
    }
}
