<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Type;
use Flow\Types\Type\Unifier\NullabilityRule;
use Flow\Types\Type\Unifier\PromotingUnifier;
use Flow\Types\Type\Unifier\StrictUnifier;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_union;

final class TypeUnifierTest extends TestCase
{
    /**
     * @return Generator<string, array{Type<mixed>, Type<mixed>, ?Type<mixed>}>
     */
    public static function promoting_pairwise_cases(): Generator
    {
        yield 'identity' => [type_string(), type_string(), type_string()];

        yield 'null is the identity' => [type_null(), type_integer(), type_integer()];

        yield 'optional operand keeps the wrapper' => [
            type_optional(type_integer()),
            type_integer(),
            type_optional(type_integer()),
        ];

        yield 'integer and float promote to float' => [type_integer(), type_float(), type_float()];

        yield 'date and datetime promote to datetime' => [type_date(), type_datetime(), type_datetime()];

        yield 'lists recurse on the element' => [
            type_list(type_integer()),
            type_list(type_float()),
            type_list(type_float()),
        ];

        yield 'integer promotes to string' => [type_integer(), type_string(), type_string()];

        yield 'datetime promotes to string' => [type_datetime(), type_string(), type_string()];

        yield 'boolean never promotes to string' => [type_boolean(), type_string(), null];

        yield 'json never promotes to string' => [type_json(), type_string(), null];

        yield 'boolean and integer have no common type' => [type_boolean(), type_integer(), null];

        yield 'list and map have no common type' => [
            type_list(type_integer()),
            type_map(type_string(), type_integer()),
            null,
        ];
    }

    /**
     * @return Generator<string, array{Type<mixed>, Type<mixed>, ?Type<mixed>}>
     */
    public static function strict_pairwise_cases(): Generator
    {
        yield 'identity' => [type_string(), type_string(), type_string()];

        yield 'null is the identity' => [type_null(), type_integer(), type_integer()];

        yield 'optional operand keeps the wrapper' => [
            type_optional(type_integer()),
            type_integer(),
            type_optional(type_integer()),
        ];

        yield 'integer and float promote to float' => [type_integer(), type_float(), type_float()];

        yield 'date and datetime promote to datetime' => [type_date(), type_datetime(), type_datetime()];

        yield 'lists recurse on the element' => [
            type_list(type_integer()),
            type_list(type_float()),
            type_list(type_float()),
        ];

        yield 'integer and string have no common type' => [type_integer(), type_string(), null];

        yield 'datetime and string have no common type' => [type_datetime(), type_string(), null];

        yield 'boolean and integer have no common type' => [type_boolean(), type_integer(), null];

        yield 'list and map have no common type' => [
            type_list(type_integer()),
            type_map(type_string(), type_integer()),
            null,
        ];
    }

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     * @param ?Type<mixed> $expected
     */
    #[DataProvider('promoting_pairwise_cases')]
    public function test_promoting_pairwise(Type $left, Type $right, ?Type $expected): void
    {
        $result = (new PromotingUnifier())->unify($left, $right);

        if ($expected === null) {
            static::assertNull($result);
        } else {
            static::assertNotNull($result);
            static::assertTrue(type_equals($expected, $result), $result->toString());
        }
    }

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     * @param ?Type<mixed> $expected
     */
    #[DataProvider('strict_pairwise_cases')]
    public function test_strict_pairwise(Type $left, Type $right, ?Type $expected): void
    {
        $result = (new StrictUnifier())->unify($left, $right);

        if ($expected === null) {
            static::assertNull($result);
        } else {
            static::assertNotNull($result);
            static::assertTrue(type_equals($expected, $result), $result->toString());
        }
    }

    public function test_null_type_is_the_identity(): void
    {
        static::assertSame(
            'integer',
            (new PromotingUnifier())
                ->unify(type_integer(), type_null())
                ?->toString(),
        );
        static::assertSame(
            'integer',
            (new StrictUnifier())
                ->unify(type_null(), type_integer())
                ?->toString(),
        );
    }

    public function test_unify_all_seeded_with_null_degenerates_to_the_first_concrete_type(): void
    {
        static::assertSame(
            'string',
            (new PromotingUnifier())
                ->unifyAll(NullabilityRule::ANY, type_string())
                ?->toString(),
        );
        static::assertSame(
            'string',
            (new StrictUnifier())
                ->unifyAll(NullabilityRule::ANY, type_string())
                ?->toString(),
        );
    }

    public function test_containers_recurse_under_the_caller_policy(): void
    {
        static::assertSame(
            'list<string>',
            (new PromotingUnifier())
                ->unify(type_list(type_integer()), type_list(type_string()))
                ?->toString(),
        );

        static::assertNull((new StrictUnifier())->unify(type_list(type_integer()), type_list(type_string())));
    }

    public function test_a_nullable_element_survives_the_recursion(): void
    {
        static::assertSame(
            'list<?float>',
            (new StrictUnifier())
                ->unify(type_list(type_optional(type_integer())), type_list(type_optional(type_float())))
                ?->toString(),
        );
    }

    public function test_map_key_never_string_promotes(): void
    {
        // U-04a.11 permits integer ⊔ float keys to unify to float, beyond the array-key template.
        // @mago-expect analysis:template-constraint-violation
        $intToFloatKey = [type_map(type_integer(), type_string()), type_map(type_float(), type_string())];
        $intToStringKey = [type_map(type_integer(), type_string()), type_map(type_string(), type_string())];

        static::assertSame(
            'map<float, string>',
            (new PromotingUnifier())
                ->unify(...$intToFloatKey)
                ?->toString(),
        );
        static::assertSame(
            'map<float, string>',
            (new StrictUnifier())
                ->unify(...$intToFloatKey)
                ?->toString(),
        );

        static::assertNull((new PromotingUnifier())->unify(...$intToStringKey));
        static::assertNull((new StrictUnifier())->unify(...$intToStringKey));
    }

    public function test_map_key_that_unifies_to_nullable_is_refused(): void
    {
        // Deliberately ill-keyed maps: the test proves the unifier refuses a key that unifies nullable.
        // @mago-expect analysis:template-constraint-violation
        static::assertNull((new PromotingUnifier())->unify(
            type_map(type_optional(type_integer()), type_string()),
            type_map(type_optional(type_float()), type_string()),
        ));
    }

    public function test_structures_with_differing_key_sets_have_no_common_type(): void
    {
        static::assertNull((new PromotingUnifier())->unify(
            type_structure(['a' => type_integer()]),
            type_structure(['a' => type_integer(), 'b' => type_string()]),
        ));
    }

    public function test_structures_with_same_fields_in_different_order_have_no_common_type(): void
    {
        static::assertNull((new PromotingUnifier())->unify(
            type_structure(['a' => type_integer(), 'b' => type_string()]),
            type_structure(['b' => type_string(), 'a' => type_integer()]),
        ));
    }

    public function test_structures_with_same_fields_in_same_order_unify(): void
    {
        $result = (new PromotingUnifier())->unify(
            type_structure(['a' => type_integer(), 'b' => type_string()]),
            type_structure(['a' => type_integer(), 'b' => type_string()]),
        );

        static::assertNotNull($result);
        static::assertSame('structure{a: integer, b: string}', $result->toString());
    }

    public function test_structure_optionality_and_allows_extra_are_or_ed(): void
    {
        $result = (new PromotingUnifier())->unify(
            type_structure(['a' => type_integer()], false),
            type_structure(['a' => structure_element('a', type_integer(), optional: true)], true),
        );

        static::assertNotNull($result);
        static::assertTrue(
            type_equals(type_structure(['a' => structure_element('a', type_integer(), optional: true)], true), $result),
            $result->toString(),
        );
    }

    public function test_promoting_unify_all_is_order_independent(): void
    {
        static::assertSame(
            'string',
            (new PromotingUnifier())
                ->unifyAll(NullabilityRule::ANY, type_datetime(), type_integer(), type_string())
                ?->toString(),
        );
        static::assertSame(
            'string',
            (new PromotingUnifier())
                ->unifyAll(NullabilityRule::ANY, type_string(), type_integer(), type_datetime())
                ?->toString(),
        );

        static::assertNull((new StrictUnifier())->unifyAll(
            NullabilityRule::ANY,
            type_datetime(),
            type_integer(),
            type_string(),
        ));
        static::assertNull((new StrictUnifier())->unifyAll(
            NullabilityRule::ANY,
            type_string(),
            type_integer(),
            type_datetime(),
        ));
    }

    public function test_coalesce_of_a_nullable_and_a_not_null_column_is_not_null(): void
    {
        static::assertSame(
            'integer',
            (new PromotingUnifier())
                ->unifyAll(NullabilityRule::ALL, type_optional(type_integer()), type_integer())
                ?->toString(),
        );

        static::assertSame(
            '?integer',
            (new PromotingUnifier())
                ->unifyAll(NullabilityRule::ANY, type_optional(type_integer()), type_integer())
                ?->toString(),
        );
    }

    public function test_the_root_and_the_element_take_different_rules(): void
    {
        static::assertSame(
            'list<?integer>',
            (new PromotingUnifier())
                ->unifyAll(
                    NullabilityRule::ALL,
                    type_optional(type_list(type_optional(type_integer()))),
                    type_list(type_integer()),
                )
                ?->toString(),
        );
    }

    public function test_unify_all_is_blind_to_the_second_nullability_spelling(): void
    {
        static::assertSame(
            '?string',
            (new PromotingUnifier())
                ->unifyAll(NullabilityRule::ANY, type_union(type_null(), type_string()), type_integer())
                ?->toString(),
        );
    }
}
