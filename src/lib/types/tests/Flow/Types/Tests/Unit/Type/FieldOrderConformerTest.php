<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Type\FieldOrderConformer;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_union;

final class FieldOrderConformerTest extends TestCase
{
    public function test_structure_fields_are_reordered_to_the_authority(): void
    {
        static::assertSame(
            'structure{a: integer, b: string}',
            (new FieldOrderConformer())
                ->conform(
                    type_structure(['b' => type_string(), 'a' => type_integer()]),
                    type_structure(['a' => type_integer(), 'b' => type_string()]),
                )
                ->toString(),
        );
    }

    public function test_each_field_keeps_its_own_optional_flag(): void
    {
        static::assertSame(
            'structure{a?: integer, b: string}',
            (new FieldOrderConformer())
                ->conform(
                    type_structure([
                        'b' => type_string(),
                        'a' => structure_element('a', type_integer(), optional: true),
                    ]),
                    type_structure(['a' => type_integer(), 'b' => type_string()]),
                )
                ->toString(),
        );
    }

    public function test_structures_inside_lists_are_conformed(): void
    {
        static::assertSame(
            'list<structure{a: integer, b: string}>',
            (new FieldOrderConformer())
                ->conform(
                    type_list(type_structure(['b' => type_string(), 'a' => type_integer()])),
                    type_list(type_structure(['a' => type_integer(), 'b' => type_string()])),
                )
                ->toString(),
        );
    }

    public function test_structures_inside_maps_are_conformed(): void
    {
        static::assertSame(
            'map<string, structure{a: integer, b: string}>',
            (new FieldOrderConformer())
                ->conform(
                    type_map(type_string(), type_structure(['b' => type_string(), 'a' => type_integer()])),
                    type_map(type_string(), type_structure(['a' => type_integer(), 'b' => type_string()])),
                )
                ->toString(),
        );
    }

    public function test_structures_under_optional_are_conformed(): void
    {
        static::assertSame(
            '?structure{a: integer, b: string}',
            (new FieldOrderConformer())
                ->conform(
                    type_optional(type_structure(['b' => type_string(), 'a' => type_integer()])),
                    type_optional(type_structure(['a' => type_integer(), 'b' => type_string()])),
                )
                ->toString(),
        );
    }

    public function test_structures_inside_unions_are_conformed_positionally(): void
    {
        static::assertSame(
            'integer|structure{a: integer, b: string}',
            (new FieldOrderConformer())
                ->conform(
                    type_union(type_structure(['b' => type_string(), 'a' => type_integer()]), type_integer()),
                    type_union(type_structure(['a' => type_integer(), 'b' => type_string()]), type_integer()),
                )
                ->toString(),
        );
    }

    public function test_structures_nested_two_levels_deep_are_conformed(): void
    {
        static::assertSame(
            'structure{outer: structure{a: integer, b: string}}',
            (new FieldOrderConformer())
                ->conform(
                    type_structure(['outer' => type_structure(['b' => type_string(), 'a' => type_integer()])]),
                    type_structure(['outer' => type_structure(['a' => type_integer(), 'b' => type_string()])]),
                )
                ->toString(),
        );
    }

    public function test_differing_name_sets_leave_the_subject_unchanged(): void
    {
        $subject = type_structure(['b' => type_string(), 'a' => type_integer()]);

        static::assertSame($subject, (new FieldOrderConformer())->conform($subject, type_structure([
            'a' => type_integer(),
            'c' => type_string(),
        ])));
    }

    public function test_a_subject_with_more_fields_than_the_authority_is_returned_unchanged_not_narrowed(): void
    {
        $subject = type_structure(['a' => type_integer(), 'b' => type_string(), 'c' => type_integer()]);

        static::assertSame($subject, (new FieldOrderConformer())->conform($subject, type_structure([
            'a' => type_integer(),
            'b' => type_string(),
        ])));
    }

    public function test_unions_with_differing_member_counts_leave_the_subject_unchanged(): void
    {
        $subject = type_union(type_structure(['b' => type_string(), 'a' => type_integer()]), type_integer());

        static::assertSame($subject, (new FieldOrderConformer())->conform($subject, type_union(
            type_structure(['a' => type_integer(), 'b' => type_string()]),
            type_integer(),
            type_string(),
        )));
    }

    public function test_differing_nullability_leaves_the_subject_unchanged(): void
    {
        $subject = type_structure(['b' => type_string(), 'a' => type_integer()]);

        static::assertSame($subject, (new FieldOrderConformer())->conform(
            $subject,
            type_optional(type_structure(['a' => type_integer(), 'b' => type_string()])),
        ));
    }
}
