<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Schema\Definition\UnionTypeNormalizer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Native\UnionType;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class UnionTypeNormalizerTest extends FlowTestCase
{
    public function test_array_member_among_many_is_normalized(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_string(), type_integer(), type_array());

        static::assertSame(
            'integer|json|string',
            (new UnionTypeNormalizer())
                ->normalize($type)
                ->toString(),
        );
    }

    public function test_array_member_is_normalized_to_json(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_string(), type_array());

        static::assertSame(
            'json|string',
            (new UnionTypeNormalizer())
                ->normalize($type)
                ->toString(),
        );
    }

    public function test_array_nested_in_a_container_member_is_normalized(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_list(type_array()), type_map(type_string(), type_array()));

        static::assertSame(
            'list<json>|map<string, json>',
            (new UnionTypeNormalizer())
                ->normalize($type)
                ->toString(),
        );
    }

    public function test_empty_array_member_is_normalized_to_json(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_list(type_string()), type_empty_array());

        static::assertSame(
            'json|list<string>',
            (new UnionTypeNormalizer())
                ->normalize($type)
                ->toString(),
        );
    }

    public function test_empty_array_nested_in_a_container_member_is_normalized(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_list(type_empty_array()), type_map(type_string(), type_empty_array()));

        static::assertSame(
            'list<json>|map<string, json>',
            (new UnionTypeNormalizer())
                ->normalize($type)
                ->toString(),
        );
    }

    public function test_optional_empty_array_member_is_normalized_to_optional_json(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_string(), type_optional(type_empty_array()));

        static::assertSame(
            'json|null|string',
            (new UnionTypeNormalizer())
                ->normalize($type)
                ->toString(),
        );
    }

    public function test_duplicate_json_members_are_not_deduplicated(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_json(), type_array());

        static::assertSame(
            2,
            (new UnionTypeNormalizer())
                ->normalize($type)
                ->types()
                ->count(),
        );
    }

    public function test_optional_array_member_is_normalized_to_optional_json(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_string(), type_optional(type_array()));

        static::assertSame(
            'json|null|string',
            (new UnionTypeNormalizer())
                ->normalize($type)
                ->toString(),
        );
    }

    public function test_union_without_array_member_is_returned_untouched(): void
    {
        /** @var UnionType<mixed, mixed> $type */
        $type = type_union(type_string(), type_optional(type_integer()));

        static::assertSame($type, (new UnionTypeNormalizer())->normalize($type));
    }
}
