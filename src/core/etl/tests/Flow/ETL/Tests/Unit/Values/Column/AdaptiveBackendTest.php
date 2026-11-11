<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Column;

use Flow\ETL\Foundation\Exception\ColumnMismatchException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\Tests\Double\ForeignColumnStub;
use Flow\ETL\Tests\Double\TrailingZerosCodecStub;
use Flow\ETL\Tests\Mother\ColumnMother;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Frame\NoopCodec;
use Flow\ETL\Values\Column\Php\PhpGroupedCounts;
use Flow\ETL\Values\Column\Php\PhpGroupedTotals;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\ValueColumn;
use Flow\ETL\Values\Grouping\PhpGroupTable;
use Flow\ETL\Values\Sort\SortColumn;
use Flow\Types\Type\Comparison\Operator;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class AdaptiveBackendTest extends TestCase
{
    protected function setUp(): void
    {
        if (extension_loaded('flow_php')) {
            static::markTestSkipped('with flow_php AdaptiveBackend is RustBackend');
        }
    }

    public function test_adopt_is_what_the_php_backend_adopts(): void
    {
        $column = ColumnMother::of(int_schema('a'), [1, 2]);
        $foreign = new ForeignColumnStub($column);

        static::assertSame($column, (new AdaptiveBackend())->adopt(int_schema('a'), $column));
        static::assertEquals(
            (new PhpBackend())->adopt(int_schema('a'), $foreign),
            (new AdaptiveBackend())->adopt(int_schema('a'), $foreign),
        );
    }

    public function test_adopt_refuses_an_untyped_column(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('column "a": mixed cannot be a batch column');

        (new AdaptiveBackend())->adopt(int_schema('a'), new ValueColumn([1]));
    }

    public function test_allocated_bytes_is_what_the_php_backend_allocates(): void
    {
        static::assertSame((new PhpBackend())->allocatedBytes(), (new AdaptiveBackend())->allocatedBytes());
    }

    public function test_builder_builds_what_the_php_backend_builds(): void
    {
        $default = (new AdaptiveBackend())->builder(str_schema('a'));
        $default->append('x');
        $php = (new PhpBackend())->builder(str_schema('a'));
        $php->append('x');

        static::assertEquals($php->finish(), $default->finish());
    }

    public function test_constant_is_what_the_php_backend_builds(): void
    {
        static::assertEquals(
            (new PhpBackend())->constant(int_schema('a'), 7, 3),
            (new AdaptiveBackend())->constant(int_schema('a'), 7, 3),
        );
    }

    public function test_decode_is_what_the_php_backend_decodes(): void
    {
        $buffers = ColumnMother::of(int_schema('a', nullable: true), [1, null])->encode();

        static::assertEquals(
            (new PhpBackend())->decode(int_schema('a', nullable: true), $buffers, 2, 1),
            (new AdaptiveBackend())->decode(int_schema('a', nullable: true), $buffers, 2, 1),
        );
    }

    public function test_decode_frame_is_what_the_php_backend_decodes(): void
    {
        $rows = RowsMother::workedExample();

        static::assertEquals(
            (new PhpBackend())->decodeFrame(
                (new PhpBackend())->encodeFrame($rows, new NoopCodec()),
                $rows->schema(),
                new NoopCodec(),
            ),
            (new AdaptiveBackend())->decodeFrame(
                (new PhpBackend())->encodeFrame($rows, new NoopCodec()),
                $rows->schema(),
                new NoopCodec(),
            ),
        );
    }

    public function test_encode_frame_delegates(): void
    {
        $rows = RowsMother::workedExample();

        static::assertSame(
            (new PhpBackend())->encodeFrame($rows, new TrailingZerosCodecStub()),
            (new AdaptiveBackend())->encodeFrame($rows, new TrailingZerosCodecStub()),
        );
    }

    public function test_compare_is_what_the_php_backend_compares(): void
    {
        $rows = array_to_rows([['a' => 1, 'b' => 2], ['a' => 2, 'b' => 2]], schema(int_schema('a'), int_schema('b')));

        static::assertEquals(
            (new PhpBackend())->compare($rows->column('a'), Operator::EQUAL, $rows->column('b'), 'a', 'b'),
            (new AdaptiveBackend())->compare($rows->column('a'), Operator::EQUAL, $rows->column('b'), 'a', 'b'),
        );
    }

    public function test_is_null_is_what_the_php_backend_answers(): void
    {
        $column = array_to_rows([['a' => 1], ['a' => null]], schema(int_schema('a', nullable: true)))->column('a');

        static::assertEquals((new PhpBackend())->isNull($column), (new AdaptiveBackend())->isNull($column));
    }

    public function test_is_not_null_is_what_the_php_backend_answers(): void
    {
        $column = array_to_rows([['a' => 1], ['a' => null]], schema(int_schema('a', nullable: true)))->column('a');

        static::assertEquals((new PhpBackend())->isNotNull($column), (new AdaptiveBackend())->isNotNull($column));
    }

    public function test_add_is_what_the_php_backend_adds(): void
    {
        $rows = array_to_rows([['a' => 7, 'b' => 2]], schema(int_schema('a'), int_schema('b')));

        static::assertEquals(
            (new PhpBackend())->add($rows->column('a'), $rows->column('b')),
            (new AdaptiveBackend())->add($rows->column('a'), $rows->column('b')),
        );
    }

    public function test_subtract_is_what_the_php_backend_subtracts(): void
    {
        $rows = array_to_rows([['a' => 7, 'b' => 2]], schema(int_schema('a'), int_schema('b')));

        static::assertEquals(
            (new PhpBackend())->subtract($rows->column('a'), $rows->column('b')),
            (new AdaptiveBackend())->subtract($rows->column('a'), $rows->column('b')),
        );
    }

    public function test_multiply_is_what_the_php_backend_multiplies(): void
    {
        $rows = array_to_rows([['a' => 7, 'b' => 2]], schema(int_schema('a'), int_schema('b')));

        static::assertEquals(
            (new PhpBackend())->multiply($rows->column('a'), $rows->column('b')),
            (new AdaptiveBackend())->multiply($rows->column('a'), $rows->column('b')),
        );
    }

    public function test_modulo_is_what_the_php_backend_answers(): void
    {
        $rows = array_to_rows([['a' => 7, 'b' => 2]], schema(int_schema('a'), int_schema('b')));

        static::assertEquals(
            (new PhpBackend())->modulo($rows->column('a'), $rows->column('b')),
            (new AdaptiveBackend())->modulo($rows->column('a'), $rows->column('b')),
        );
    }

    public function test_sort_is_what_the_php_backend_sorts(): void
    {
        $rows = array_to_rows([['a' => 2], ['a' => 1]], schema(int_schema('a')));

        static::assertSame(
            (new PhpBackend())
                ->sort($rows, [new SortColumn('a')])
                ->toArray(),
            (new AdaptiveBackend())
                ->sort($rows, [new SortColumn('a')])
                ->toArray(),
        );
    }

    public function test_sort_working_bytes_are_what_the_php_backend_needs(): void
    {
        $rows = array_to_rows([['a' => 2], ['a' => 1]], schema(int_schema('a')));

        static::assertSame(
            (new PhpBackend())->sortWorkingBytes($rows, [new SortColumn('a')]),
            (new AdaptiveBackend())->sortWorkingBytes($rows, [new SortColumn('a')]),
        );
    }

    public function test_grouped_totals_counts_and_table_are_the_php_backends(): void
    {
        static::assertInstanceOf(PhpGroupedTotals::class, (new AdaptiveBackend())->groupedTotals());
        static::assertInstanceOf(PhpGroupedCounts::class, (new AdaptiveBackend())->groupedCounts());
        static::assertInstanceOf(PhpGroupTable::class, (new AdaptiveBackend())->groupTable(
            ['a'],
            schema(int_schema('a')),
        ));
    }

    public function test_a_flow_php_of_another_abi_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('does not match flow-php/etl');

        new AdaptiveBackend(new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'));
    }
}
