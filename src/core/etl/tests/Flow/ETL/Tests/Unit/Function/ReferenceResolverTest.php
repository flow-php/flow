<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row\ResolvedReference;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_is_nullable;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class ReferenceResolverTest extends FlowTestCase
{
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
        $tree = ref('list')->onEach(ref('element')->upper());

        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve($tree, schema(
            list_schema('list', type_list(type_string())),
            str_schema('element'),
        ));

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

        $schema = schema(int_schema('id'), datetime_schema('created_at'));

        /** @var ScalarFunction $predicate */
        $predicate = (new ReferenceResolver())->resolve(ref('id')->greaterThan(ref('created_at')), $schema);
        $predicate->returns();
    }

    public function test_assert_resolved_names_the_column_and_the_available_ones(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage(
            'Schema definition for entry "scoree" not found. Available columns: [id, score].',
        );

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
}
