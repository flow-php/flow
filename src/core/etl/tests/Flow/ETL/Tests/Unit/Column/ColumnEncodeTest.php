<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column;

use DateTimeImmutable;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\Mother\ColumnMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_map;
use function bin2hex;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ColumnEncodeTest extends TestCase
{
    /**
     * @return Generator<string, array{Column, list<string>}>
     */
    public static function columns(): Generator
    {
        yield 'integer' => [
            ColumnMother::of(int_schema('id'), [1, 2]),
            ['', '01000000000000000200000000000000'],
        ];
        yield 'nullable string' => [
            ColumnMother::of(str_schema('name', nullable: true), ['ab', null]),
            ['01', '000000000200000002000000', '6162'],
        ];
        yield 'float' => [ColumnMother::of(float_schema('a'), [1.5]), ['', '000000000000f83f']];
        yield 'date before the epoch' => [
            ColumnMother::of(date_schema('a'), [new DateTimeImmutable('1969-12-29 00:00:00 UTC')]),
            ['', 'fdffffff'],
        ];
        yield 'boolean' => [ColumnMother::of(bool_schema('a'), [true, false, true]), ['', '05']];
        yield 'nullable boolean with a null slot at 0' => [
            ColumnMother::of(bool_schema('a', nullable: true), [true, null, true]),
            ['05', '05'],
        ];
        yield 'uuid' => [
            ColumnMother::of(uuid_schema('a'), ['6c2f1d4e-8b3a-4c5d-9e6f-0a1b2c3d4e5f']),
            ['', '6c2f1d4e8b3a4c5d9e6f0a1b2c3d4e5f'],
        ];
        yield 'list with a null slot' => [
            ColumnMother::of(list_schema('a', type_list(type_integer()), nullable: true), [[1], null, [2, 3]]),
            ['05', '00000000010000000100000003000000', '', '010000000000000002000000000000000300000000000000'],
        ];
        yield 'map' => [
            ColumnMother::of(map_schema('a', type_map(type_string(), type_integer())), [['a' => 1]]),
            ['', '0000000001000000', '', '', '0000000001000000', '61', '', '0100000000000000'],
        ];
        yield 'structure: the child bit is 0 under a null parent' => [
            ColumnMother::of(structure_schema('a', type_structure(['x' => type_integer()]), nullable: true), [
                ['x' => 1],
                null,
            ]),
            ['01', '01', '01000000000000000000000000000000'],
        ];
        yield 'a null column has no buffers' => [ColumnMother::of(null_schema('a'), [null, null, null]), []];
        yield 'a null element of a structure has no buffers' => [
            ColumnMother::of(structure_schema('a', type_structure(['x' => type_integer(), 'n' => type_null()])), [
                ['x' => 1, 'n' => null],
            ]),
            ['', '', '0100000000000000'],
        ];
        yield 'an optional null element of a structure has no buffers' => [
            ColumnMother::of(
                structure_schema('a', type_structure(['x' => type_integer(), 'n' => type_optional(type_null())])),
                [['x' => 1, 'n' => null]],
            ),
            ['', '', '0100000000000000'],
        ];
        yield 'a constant is expanded' => [
            (new PhpBackend())->constant(int_schema('a'), 7, 2),
            ['', '07000000000000000700000000000000'],
        ];
        yield 'a slice re-bases offsets and re-packs validity from bit 0' => [
            ColumnMother::of(list_schema('a', type_list(type_integer()), nullable: true), [
                [1],
                [2, 3],
                null,
            ])->slice(1, 2),
            ['01', '000000000200000002000000', '', '02000000000000000300000000000000'],
        ];
    }

    /**
     * @param list<string> $hex
     */
    #[DataProvider('columns')]
    public function test_encodes(Column $column, array $hex): void
    {
        static::assertSame($hex, array_map(bin2hex(...), $column->encode()));
    }
}
