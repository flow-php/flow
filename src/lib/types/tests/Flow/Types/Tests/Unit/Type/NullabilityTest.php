<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Type;
use Flow\Types\Type\Nullability;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

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
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;

final class NullabilityTest extends TestCase
{
    /**
     * @return Generator<string, array{Type<mixed>}>
     */
    public static function both_nullability_spellings_of_every_column_representable_type(): Generator
    {
        $columnRepresentable = [
            'boolean' => type_boolean(),
            'integer' => type_integer(),
            'float' => type_float(),
            'string' => type_string(),
            'null' => type_null(),
            'json' => type_json(),
            'uuid' => type_uuid(),
            'datetime' => type_datetime(),
            'date' => type_date(),
            'time' => type_time(),
            'xml' => type_xml(),
            'list<integer>' => type_list(type_integer()),
            'map<string,integer>' => type_map(type_string(), type_integer()),
            'structure{a:integer}' => type_structure(['a' => type_integer()]),
        ];

        foreach ($columnRepresentable as $name => $type) {
            yield $name => [$type];

            yield '?' . $name => [type_optional($type)];

            yield 'union{null,' . $name . '}' => [type_union(type_null(), $type)];
        }
    }

    public function test_is_recognises_both_spellings(): void
    {
        static::assertTrue((new Nullability())->is(type_optional(type_integer())));
        static::assertTrue((new Nullability())->is(type_union(type_null(), type_string())));

        static::assertFalse((new Nullability())->is(type_integer()));
        static::assertFalse((new Nullability())->is(type_null()));
        static::assertFalse((new Nullability())->is(type_list(type_optional(type_integer()))));
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('both_nullability_spellings_of_every_column_representable_type')]
    public function test_bare_is_the_dual_of_is(Type $type): void
    {
        static::assertFalse((new Nullability())->is((new Nullability())->bare($type)));
    }

    public function test_bare_is_lossless_for_a_multi_member_nullable_union(): void
    {
        static::assertTrue(type_equals(
            type_union(type_integer(), type_string()),
            (new Nullability())->bare(type_union(type_null(), type_integer(), type_string())),
        ));
    }

    public function test_bare_leaves_a_not_null_type_identical(): void
    {
        $type = type_list(type_optional(type_integer()));

        static::assertSame($type, (new Nullability())->bare($type));
    }

    public function test_any_wraps_when_one_operand_is_nullable(): void
    {
        static::assertSame(
            '?boolean',
            (new Nullability())->any(type_boolean(), type_optional(type_integer()), type_string())->toString(),
        );
    }

    public function test_any_leaves_a_not_null_result_bare(): void
    {
        static::assertSame(
            'boolean',
            (new Nullability())->any(type_boolean(), type_integer(), type_string())->toString(),
        );
    }

    public function test_all_wraps_only_when_every_operand_is_nullable(): void
    {
        static::assertSame(
            '?integer',
            (new Nullability())->all(
                type_integer(),
                type_optional(type_integer()),
                type_optional(type_string()),
            )->toString(),
        );

        static::assertSame(
            'integer',
            (new Nullability())->all(type_integer(), type_optional(type_integer()), type_string())->toString(),
        );
    }

    public function test_all_over_zero_operands_is_not_nullable(): void
    {
        static::assertSame('integer', (new Nullability())->all(type_integer())->toString());
    }
}
