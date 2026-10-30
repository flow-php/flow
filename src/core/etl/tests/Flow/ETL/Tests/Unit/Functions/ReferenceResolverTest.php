<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Functions\Between\Boundary;
use Flow\ETL\Functions\Literal;
use Flow\ETL\Functions\Reference\ResolvedReference;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_expand;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_is_nullable;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class ReferenceResolverTest extends FlowTestCase
{
    public static function temporal_comparisons_with_a_string(): Generator
    {
        yield 'equals' => [ref('ts')->equals(lit('2026-10-02')), 'boolean'];
        yield 'not equals' => [ref('ts')->notEquals(lit('2026-10-02')), 'boolean'];
        yield 'greater than' => [ref('ts')->greaterThan(lit('2026-10-02')), 'boolean'];
        yield 'greater than equal' => [ref('ts')->greaterThanEqual(lit('2026-10-02')), 'boolean'];
        yield 'less than' => [ref('ts')->lessThan(lit('2026-10-02')), 'boolean'];
        yield 'less than equal' => [ref('ts')->lessThanEqual(lit('2026-10-02')), 'boolean'];
        yield 'string on the left' => [lit('2026-10-02')->lessThan(ref('ts')), 'boolean'];
        yield 'date column' => [ref('d')->equals(lit('2026-10-02')), 'boolean'];
        yield 'string column' => [ref('ts')->equals(ref('s')), 'boolean'];
        yield 'nullable string column' => [ref('ts')->equals(ref('n')), '?boolean'];
        yield 'between' => [ref('ts')->between(lit('2026-10-02'), lit('2026-10-03'), Boundary::INCLUSIVE), 'boolean'];
        yield 'is in' => [ref('ts')->isIn(lit(['2026-10-02'])), 'boolean'];
    }

    #[DataProvider('temporal_comparisons_with_a_string')]
    public function test_a_string_compared_with_a_temporal_operand_is_cast_to_it(
        ScalarFunction $predicate,
        string $expected,
    ): void {
        static::assertSame(
            $expected,
            (new ReferenceResolver())
                ->resolve($predicate, schema(
                    datetime_schema('ts'),
                    date_schema('d'),
                    str_schema('s'),
                    str_schema('n', nullable: true),
                ))
                ->returns()
                ->toString(),
        );
    }

    public function test_same_keeps_a_string_and_a_datetime_strict(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            ref('ts')->same(lit('2026-10-02')),
            schema(datetime_schema('ts')),
        );

        static::assertInstanceOf(Literal::class, $resolved->children()[1]);
    }

    public function test_a_coerced_comparison_resolves_to_itself(): void
    {
        $schema = schema(datetime_schema('ts'), str_schema('s'));
        $resolved = (new ReferenceResolver())->resolve(ref('ts')->equals(ref('s')), $schema);

        static::assertSame($resolved, (new ReferenceResolver())->resolve($resolved, $schema));
    }

    public function test_a_comparison_over_an_unknown_column_is_left_as_it_is(): void
    {
        $predicate = ref('missing')->equals(lit('2026-10-02'));

        static::assertSame($predicate, (new ReferenceResolver())->resolve($predicate, schema(datetime_schema('ts'))));
    }

    public function test_resolves_a_leaf_from_a_hand_built_schema(): void
    {
        /** @var ResolvedReference $resolved */
        $resolved = (new ReferenceResolver())->resolve(ref('a'), schema(int_schema('a')));

        static::assertInstanceOf(ResolvedReference::class, $resolved);
        static::assertSame('integer', $resolved->returns()->toString());
        static::assertFalse(type_is_nullable($resolved->returns()));
    }

    public function test_resolves_a_leaf_inside_a_nested_tree(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(ref('a')->upper()->equals(lit('X')), schema(str_schema('a')));

        static::assertTrue($resolved->resolved());
        static::assertSame('boolean', $resolved->returns()->toString());
    }

    public function test_returns_the_same_object_when_nothing_moved(): void
    {
        $schema = schema(str_schema('a'));
        $resolved = (new ReferenceResolver())->resolve(ref('a')->upper(), $schema);

        static::assertSame($resolved, (new ReferenceResolver())->resolve($resolved, $schema));
    }

    public function test_an_unknown_column_is_left_unresolved(): void
    {
        $resolved = (new ReferenceResolver())->resolve(ref('missing'), schema(str_schema('a')));

        static::assertInstanceOf(UnresolvedReference::class, $resolved);
        static::assertFalse($resolved->resolved());
    }

    public function test_the_original_tree_is_not_mutated(): void
    {
        $tree = ref('a')->upper();

        (new ReferenceResolver())->resolve($tree, schema(str_schema('a')));

        static::assertFalse($tree->resolved());
        static::assertInstanceOf(UnresolvedReference::class, $tree->children()[0]);
    }

    public function test_a_lambda_body_is_not_walked(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            ref('list')->onEach(ref('element')->upper()),
            schema(list_schema('list', type_list(type_string())), str_schema('element')),
        );

        static::assertSame('list<string>', $resolved->returns()->toString());
    }

    public function test_an_exists_operand_is_not_walked(): void
    {
        $tree = ref('missing')->exists();

        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve($tree, schema(str_schema('a')));

        static::assertSame($tree, $resolved);
        static::assertTrue($resolved->resolved());
        static::assertSame('boolean', $resolved->returns()->toString());
    }

    public function test_an_impossible_comparison_fails_at_bind(): void
    {
        // Until 04b wires the DataFrame bind, the bind moment is resolve() + returns().
        $this->expectExceptionMessage("Can't compare");

        /** @var ScalarFunction $predicate */
        $predicate = (new ReferenceResolver())->resolve(
            ref('id')->greaterThan(ref('created_at')),
            schema(int_schema('id'), datetime_schema('created_at')),
        );
        $predicate->returns();
    }

    public function test_assert_resolved_names_the_column_and_the_available_ones(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for column "scoree" not found. Did you mean one of: [score]?');

        $schema = schema(int_schema('id'), int_schema('score'));

        (new ReferenceResolver())->assertResolved(
            (new ReferenceResolver())->resolve(ref('scoree')->greaterThan(lit(10)), $schema),
            $schema,
        );
    }

    public function test_assert_resolved_passes_for_a_fully_resolved_tree(): void
    {
        $schema = schema(int_schema('score'));

        (new ReferenceResolver())->assertResolved(
            (new ReferenceResolver())->resolve(ref('score')->greaterThan(lit(10)), $schema),
            $schema,
        );

        $this->expectNotToPerformAssertions();
    }

    public function test_an_aliased_reference_resolves_by_its_source_column(): void
    {
        $resolved = (new ReferenceResolver())->resolve(ref('age')->as('total_age'), schema(int_schema('age')));

        static::assertInstanceOf(ResolvedReference::class, $resolved);
        static::assertSame('total_age', $resolved->name());
        static::assertSame('age', $resolved->to());
        static::assertSame('integer', $resolved->returns()->toString());
    }

    public function test_bind_returns_the_resolved_tree(): void
    {
        static::assertInstanceOf(ResolvedReference::class, (new ReferenceResolver())->bind(
            ref('id'),
            schema(int_schema('id')),
            'transform',
        ));
    }

    public function test_bind_refuses_an_unknown_column(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new ReferenceResolver())->bind(ref('missing'), schema(int_schema('id')), 'transform');
    }

    public function test_bind_refuses_an_expansion_naming_the_method(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('over() cannot contain array_expand()');

        (new ReferenceResolver())->bind(
            array_expand(ref('list')),
            schema(list_schema('list', type_list(type_string()))),
            'over',
        );
    }

    public function test_bind_refuses_a_result_of_another_type_than_required(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('until() requires a predicate returning boolean');

        (new ReferenceResolver())->bind(ref('id'), schema(int_schema('id')), 'until', type_boolean());
    }

    public function test_bind_accepts_a_nullable_result_of_the_required_type(): void
    {
        static::assertSame(
            '?boolean',
            (new ReferenceResolver())
                ->bind(ref('id')->equals(lit(1)), schema(int_schema('id', nullable: true)), 'filter', type_boolean())
                ->returns()
                ->toString(),
        );
    }
}
