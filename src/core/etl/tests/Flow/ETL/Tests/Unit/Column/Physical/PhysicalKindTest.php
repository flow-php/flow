<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use Flow\ETL\Column\Physical\PhysicalKind;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;
use function str_repeat;

final class PhysicalKindTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{Type<mixed>, mixed, bool}>
     */
    public static function physicals(): Generator
    {
        yield 'null is any type\'s' => [type_integer(), null, true];
        yield 'int in an integer column' => [type_integer(), 1, true];
        yield 'string in an integer column' => [type_integer(), '1', false];
        yield 'int in a float column' => [type_float(), 1, false];
        yield 'float in a float column' => [type_float(), 1.5, true];
        yield 'bool' => [type_boolean(), true, true];
        yield 'datetime micros' => [type_datetime(), 1_700_000_000_000_000, true];
        yield 'datetime text' => [type_datetime(), '2026-01-01', false];
        yield 'uuid 16 bytes' => [type_uuid(), str_repeat("\0", 16), true];
        yield 'uuid text' => [type_uuid(), '00000000-0000-4000-8000-000000000000', false];
        yield 'json text' => [type_json(), '{"a":1}', true];
        yield 'optional unwrapped' => [type_optional(type_integer()), 1, true];
        yield 'anything in a null column' => [type_null(), 'x', true];
        yield 'list of ints' => [type_list(type_integer()), [1, null, 2], true];
        yield 'list with a wrong element' => [type_list(type_integer()), [1, '2'], false];
        yield 'list that is not an array' => [type_list(type_integer()), 'x', false];
        yield 'map values checked' => [type_map(type_string(), type_integer()), ['a' => 'b'], false];
        yield 'structure element checked' => [type_structure(['a' => type_integer()]), ['a' => 'x'], false];
        yield 'structure element absent' => [type_structure(['a' => type_integer()]), [], true];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('physicals')]
    public function test_accepts_the_kind_flow_php_stores(Type $type, mixed $physical, bool $accepts): void
    {
        static::assertSame($accepts, (new PhysicalKind())->accepts($type, $physical));
    }
}
