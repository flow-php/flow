<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type;

use function Flow\ETL\DSL\{type_boolean, type_float, type_int, type_list, type_map, type_string};
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class TypesCompatibilityTest extends FlowTestCase
{
    public static function list_compatibility_provider() : \Generator
    {
        yield [type_list(type_int()), type_list(type_int()), true];
        yield [type_list(type_int()), type_list(type_int(), true), false];
        yield [type_list(type_int(), true), type_list(type_int()), true];
        yield [type_list(type_int(), true), type_list(type_int(), true), true];

        yield [type_list(type_int()), type_list(type_string()), false];

        yield [type_list(type_int()), type_list(type_int(true)), false];
    }

    public static function map_compatibility_provider() : \Generator
    {
        yield [type_map(type_string(), type_int()), type_map(type_string(), type_int()), true];
        yield [type_map(type_string(), type_int()), type_map(type_string(), type_int(), true), false];
        yield [type_map(type_string(), type_int(), true), type_map(type_string(), type_int()), true];

        yield [type_map(type_string(), type_int(), true), type_map(type_string(), type_int(), true), true];
        yield [type_map(type_string(), type_int()), type_map(type_string(), type_string()), false];
        yield [type_map(type_string(), type_int()), type_map(type_string(), type_int(true)), false];
        yield [type_map(type_string(), type_int(true)), type_map(type_string(), type_int()), true];
    }

    public static function scalar_types_compatibility_provider() : \Generator
    {
        yield [type_int(), type_int(), true];
        yield [type_int(true), type_int(), true];
        yield [type_int(), type_int(true), false];
        yield [type_int(true), type_int(true), true];

        yield [type_string(), type_string(), true];
        yield [type_string(true), type_string(), true];
        yield [type_string(), type_string(true), false];
        yield [type_string(true), type_string(true), true];

        yield [type_float(), type_float(), true];
        yield [type_float(true), type_float(), true];
        yield [type_float(), type_float(true), false];
        yield [type_float(true), type_float(true), true];

        yield [type_boolean(), type_boolean(), true];
        yield [type_boolean(true), type_boolean(), true];
        yield [type_boolean(), type_boolean(true), false];
        yield [type_boolean(true), type_boolean(true), true];
    }

    /**
     * @param Type<mixed> $given
     * @param Type<mixed> $expected
     */
    #[DataProvider('list_compatibility_provider')]
    public function test_list_type_compatibility(Type $given, Type $expected, bool $compatible) : void
    {
        self::assertSame($compatible, $given->isCompatibleWith($expected));
    }

    /**
     * @param Type<mixed> $given
     * @param Type<mixed> $expected
     */
    #[DataProvider('map_compatibility_provider')]
    public function test_map_type_compatibility(Type $given, Type $expected, bool $compatible) : void
    {
        self::assertSame($compatible, $given->isCompatibleWith($expected));
    }

    /**
     * @param Type<mixed> $given
     * @param Type<mixed> $expected
     */
    #[DataProvider('scalar_types_compatibility_provider')]
    public function test_scalar_type_compatibility(Type $given, Type $expected, bool $compatible) : void
    {
        self::assertSame($compatible, $given->isCompatibleWith($expected));
    }
}
