<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use Flow\Floe\Codec;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\FrameDecoder;
use Flow\Floe\FrameEncoder;
use Flow\Floe\Tests\Context\FrameContext;
use Flow\Floe\Tests\Double\PrefixingCodecStub;
use Flow\Floe\Tests\Double\TrailingZerosCodecStub;
use Flow\Floe\Tests\Mother\RowsMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function pack;
use function substr;
use function substr_replace;

final class FrameDecoderTest extends TestCase
{
    /**
     * @return Generator<string, array{Rows, Codec}>
     */
    public static function round_trips(): Generator
    {
        $cases = [
            'all entry types' => RowsMother::withAllEntryTypes(),
            'worked example' => RowsMother::workedExample(),
            'list with a null slot' => array_to_rows([
                ['l' => [1, 2]],
                ['l' => null],
                ['l' => []],
            ], schema(list_schema('l', type_list(type_integer()), nullable: true))),
            'map' => array_to_rows([
                ['m' => ['a' => 1]],
                ['m' => ['b' => 2, 'c' => 3]],
            ], schema(map_schema('m', type_map(type_string(), type_integer())))),
            'structure with a null parent' => array_to_rows([
                ['s' => ['a' => 1]],
                ['s' => null],
            ], schema(structure_schema('s', type_structure(['a' => type_integer()]), nullable: true))),
            'null column' => array_to_rows([['n' => null], ['n' => null]], schema(null_schema('n'))),
            'list of nulls' => array_to_rows([
                ['l' => [null, null]],
                ['l' => [null]],
            ], schema(list_schema('l', type_list(type_null())))),
            'map of null values' => array_to_rows([['m' => [
                'a' => null,
            ]]], schema(map_schema('m', type_map(type_string(), type_null())))),
            'no columns' => array_to_rows([[], [], []], schema()),
        ];

        foreach ($cases as $name => $rows) {
            yield $name . ' / noop' => [$rows, new NoopCodec()];
            yield $name . ' / trailing zeros' => [$rows, new TrailingZerosCodecStub()];
            yield $name . ' / prefixing' => [$rows, new PrefixingCodecStub()];
        }
    }

    #[DataProvider('round_trips')]
    public function test_round_trips(Rows $rows, Codec $codec): void
    {
        $decoded = (new FrameDecoder($codec))->decode(
            (new FrameEncoder($codec))->encode($rows),
            $rows->schema(),
            new PhpBackend(),
        );

        static::assertSame($rows->count(), $decoded->count());
        static::assertEquals($rows->toArray(), $decoded->toArray());
    }

    public function test_a_numeric_column_name_round_trips(): void
    {
        $rows = array_to_rows([['0' => 1], ['0' => null]], schema(int_schema('0', nullable: true)));

        static::assertSame(
            $rows->toArray(),
            (new FrameDecoder())
                ->decode($rows->encodeFrame(), $rows->schema(), new PhpBackend())
                ->toArray(),
        );
    }

    public function test_a_null_struct_child_under_a_null_parent_is_accepted(): void
    {
        $rows = array_to_rows([
            ['s' => ['a' => 1, 'b' => 'x']],
            ['s' => null],
        ], schema(structure_schema('s', type_structure(['a' => type_integer(), 'b' => type_string()]), nullable: true)));

        static::assertSame(
            $rows->toArray(),
            (new FrameDecoder())
                ->decode($rows->encodeFrame(), $rows->schema(), new PhpBackend())
                ->toArray(),
        );
    }

    /**
     * @return Generator<string, array{string, string}>
     */
    public static function refusals(): Generator
    {
        $body = RowsMother::workedExample()->encodeFrame();

        yield 'a body shorter than the directory counts' => [
            substr($body, 0, 11),
            'Floe BATCH frame is truncated, its directory is incomplete',
        ];
        yield 'a directory cut short' => [
            substr($body, 0, 40),
            'Floe BATCH frame is truncated, its directory is incomplete',
        ];
        yield 'a node count other than the schema' => [
            FrameContext::patch($body, [1 => 3]),
            'Floe BATCH frame holds 3 nodes, the schema describes 2',
        ];
        yield 'a buffer count other than the schema' => [
            FrameContext::patch($body, [2 => 4]),
            'Floe BATCH frame holds 4 buffers, the schema describes 5',
        ];
        yield 'a column of another row count' => [
            FrameContext::patch($body, [3 => 3]),
            'Floe BATCH frame column "id" holds 3 rows, the frame 2',
        ];
        yield 'more nulls than rows' => [
            FrameContext::patch($body, [4 => 3]),
            'Floe BATCH frame column "id" has 3 nulls in 2 rows',
        ];
        yield 'a buffer past the area' => [
            FrameContext::patch($body, [10 => 200]),
            'Floe BATCH frame buffer 1 lies outside the 80-byte buffer area',
        ];
        yield 'a buffer shorter than its prefix' => [
            FrameContext::patch($body, [10 => 4]),
            'Floe BATCH frame buffer 1 is shorter than its 8-byte length prefix',
        ];
        yield 'a null count other than the bitmap' => [
            FrameContext::patch($body, [6 => 0]),
            'Floe BATCH frame column "name" is malformed: Column null count 0 disagrees with its validity bitmap (1 nulls in 2 rows)',
        ];
    }

    #[DataProvider('refusals')]
    public function test_refuses(string $body, string $message): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage($message);

        (new FrameDecoder())->decode($body, RowsMother::workedExample()->schema(), new PhpBackend());
    }

    public function test_refuses_a_buffer_that_decodes_to_another_length_than_its_prefix(): void
    {
        $body = (new FrameEncoder(new TrailingZerosCodecStub()))->encode(RowsMother::workedExample());

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe BATCH frame buffer 1 decodes to 16 bytes, its prefix declares 15');

        (new FrameDecoder(new TrailingZerosCodecStub()))->decode(
            substr_replace($body, pack('P', 15), 72, 8),
            RowsMother::workedExample()->schema(),
            new PhpBackend(),
        );
    }

    public function test_refuses_child_nodes_that_disagree_with_the_buffers(): void
    {
        $rows = array_to_rows([
            ['l' => [1, 2]],
            ['l' => null],
        ], schema(list_schema('l', type_list(type_integer()), nullable: true)));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe BATCH frame column "l" declares nodes that disagree with its buffers');

        (new FrameDecoder())->decode(
            FrameContext::patch($rows->encodeFrame(), [5 => 3]),
            $rows->schema(),
            new PhpBackend(),
        );
    }

    public function test_refuses_a_backend_refusal_as_malformed(): void
    {
        $body = FrameContext::patch(RowsMother::workedExample()->encodeFrame(), [10 => 10]);

        try {
            (new FrameDecoder())->decode($body, RowsMother::workedExample()->schema(), new PhpBackend());
            static::fail('A cut values buffer must be refused');
        } catch (FloeException $e) {
            static::assertSame(
                'Floe BATCH frame column "id" is malformed: Int64 values buffer of 2 bytes, expected 16 for 2 rows',
                $e->getMessage(),
            );
            static::assertInstanceOf(InvalidArgumentException::class, $e->getPrevious());
        }
    }

    public function test_a_not_null_column_with_a_null_node_is_refused(): void
    {
        $body = array_to_rows([['id' => 1], ['id' => null]], schema(int_schema('id', nullable: true)))->encodeFrame();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage(
            'Floe BATCH frame is malformed: Rows do not match their schema: column "id" (row 1): could not convert null to integer, column is not nullable',
        );

        (new FrameDecoder())->decode($body, schema(int_schema('id')), new PhpBackend());
    }

    public function test_a_null_in_a_non_optional_list_element_is_refused(): void
    {
        $body = array_to_rows([['l' => [
            1,
            null,
        ]]], schema(list_schema('l', type_list(type_optional(type_integer())))))->encodeFrame();

        try {
            (new FrameDecoder())->decode($body, schema(list_schema('l', type_list(type_integer()))), new PhpBackend());
            static::fail('A null list element must be refused');
        } catch (FloeException $e) {
            static::assertSame(
                'Floe BATCH frame column "l" is malformed: List child holds 1 nulls in a non-nullable integer',
                $e->getMessage(),
            );
            static::assertInstanceOf(InvalidArgumentException::class, $e->getPrevious());
        }
    }

    public function test_a_null_map_key_is_refused(): void
    {
        $rows = array_to_rows([['m' => [
            1 => null,
        ]]], schema(map_schema('m', type_map(type_integer(), type_optional(type_integer())))));
        $words = FrameContext::directory($rows->encodeFrame());
        $body = FrameContext::patch($rows->encodeFrame(), [
            8 => 1,
            10 => 0,
            17 => $words[21],
            18 => $words[22],
            21 => $words[17],
            22 => $words[18],
        ]);

        try {
            (new FrameDecoder())->decode($body, $rows->schema(), new PhpBackend());
            static::fail('A null map key must be refused');
        } catch (FloeException $e) {
            static::assertSame(
                'Floe BATCH frame column "m" is malformed: Map key child holds 1 nulls in a non-nullable integer',
                $e->getMessage(),
            );
            static::assertInstanceOf(InvalidArgumentException::class, $e->getPrevious());
        }
    }
}
