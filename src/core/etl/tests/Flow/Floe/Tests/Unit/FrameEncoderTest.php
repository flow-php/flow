<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\FrameEncoder;
use Flow\Floe\Tests\Context\FrameContext;
use Flow\Floe\Tests\Double\PrefixingCodecStub;
use Flow\Floe\Tests\Double\TrailingZerosCodecStub;
use Flow\Floe\Tests\Mother\RowsMother;
use PHPUnit\Framework\TestCase;

use function bin2hex;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class FrameEncoderTest extends TestCase
{
    public function test_the_worked_example(): void
    {
        static::assertSame(
            '020000000200000005000000020000000000000002000000010000000000000000000000000000001800000018000000090000002800000014000000400000000a00000000000000ffffffffffffffff01000000000000000200000000000000ffffffffffffffff0100000000000000ffffffffffffffff00000000020000000200000000000000ffffffffffffffff6162000000000000',
            bin2hex((new FrameEncoder())->encode(RowsMother::workedExample())),
        );
    }

    public function test_a_codec_wraps_each_buffer_behind_its_length(): void
    {
        static::assertSame(
            '020000000200000005000000020000000000000002000000010000000000000000000000000000001200000018000000090000002800000012000000400000000a00000000000000100000000000000001000000000000000207000000000000ffffffffffffffff01000000000000000c0000000000000000000000020000000203000000000000ffffffffffffffff6162000000000000',
            bin2hex((new FrameEncoder(new TrailingZerosCodecStub()))->encode(RowsMother::workedExample())),
        );
    }

    public function test_a_codec_whose_output_is_not_smaller_stores_raw(): void
    {
        static::assertSame(
            (new FrameEncoder())->encode(RowsMother::workedExample()),
            (new FrameEncoder(new PrefixingCodecStub()))->encode(RowsMother::workedExample()),
        );
    }

    public function test_a_list_with_a_null_slot(): void
    {
        $body = (new FrameEncoder())->encode(array_to_rows([
            ['l' => [1, 2]],
            ['l' => null],
        ], schema(list_schema('l', type_list(type_integer()), nullable: true))));

        static::assertSame([2, 2, 4, 2, 1, 2, 0, 0, 9, 16, 20, 0, 0, 40, 24], FrameContext::directory($body));
    }

    public function test_a_map_entries_node_and_extent(): void
    {
        $body = (new FrameEncoder())->encode(array_to_rows([
            ['m' => ['a' => 1]],
            ['m' => ['b' => 2, 'c' => 3]],
        ], schema(map_schema('m', type_map(type_string(), type_integer())))));

        static::assertSame(
            [2, 4, 8, 2, 0, 3, 0, 3, 0, 3, 0, 0, 0, 0, 20, 0, 0, 0, 0, 24, 24, 48, 11, 0, 0, 64, 32],
            FrameContext::directory($body),
        );
    }

    public function test_a_structure_with_a_null_parent(): void
    {
        $body = (new FrameEncoder())->encode(array_to_rows([
            ['s' => ['a' => 1]],
            ['s' => null],
        ], schema(structure_schema('s', type_structure(['a' => type_integer()]), nullable: true))));

        static::assertSame([2, 2, 3, 2, 1, 2, 1, 0, 9, 16, 9, 32, 24], FrameContext::directory($body));
    }

    public function test_a_null_column_has_a_full_null_node_and_no_extent(): void
    {
        static::assertSame(
            '020000000100000000000000020000000200000000000000',
            bin2hex((new FrameEncoder())->encode(array_to_rows([
                ['n' => null],
                ['n' => null],
            ], schema(null_schema('n'))))),
        );
    }

    public function test_a_batch_without_columns_keeps_its_row_count(): void
    {
        static::assertSame(
            '03000000000000000000000000000000',
            bin2hex((new FrameEncoder())->encode(array_to_rows([[], [], []], schema()))),
        );
    }
}
