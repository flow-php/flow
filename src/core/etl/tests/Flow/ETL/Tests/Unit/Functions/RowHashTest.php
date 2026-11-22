<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\RowHash;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row_hash;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_string;

final class RowHashTest extends FlowTestCase
{
    public function test_it_hashes_the_named_pairs_of_each_row(): void
    {
        static::assertSame(
            ['bdea5ee4847f3df6cb724988fb66ecd4', 'ba7c3390ce5fcedfea5c7d697dbe1ac9'],
            (new FunctionContext(flow_context()))
                ->column(
                    row_hash(ref('a'), ref('b')),
                    [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'y']],
                    schema(int_schema('a'), str_schema('b')),
                )
                ->values(),
        );
    }

    public function test_it_equals_the_row_hashes_of_the_backend(): void
    {
        $rows = array_to_rows(
            [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'y']],
            schema(int_schema('a'), str_schema('b')),
        );

        static::assertSame(
            (new PhpBackend())
                ->rowHashes($rows->columns(), 2)
                ->values(),
            row_hash(ref('a'), ref('b'))->eval($rows, new PhpBackend())->values(),
        );
    }

    public function test_the_order_of_the_references_does_not_count(): void
    {
        $rows = array_to_rows([['a' => 1, 'b' => 'x']], schema(int_schema('a'), str_schema('b')));

        static::assertSame(
            row_hash(ref('a'), ref('b'))->eval($rows, new PhpBackend())->values(),
            row_hash(ref('b'), ref('a'))->eval($rows, new PhpBackend())->values(),
        );
    }

    public function test_a_renamed_column_gives_another_hash(): void
    {
        $original = array_to_rows([['a' => 1]], schema(int_schema('a')));
        $renamed = array_to_rows([['c' => 1]], schema(int_schema('c')));

        static::assertSame(
            ['8e18789eac90baf2a1f6bc7cceafa11a'],
            row_hash(ref('a'))->eval($original, new PhpBackend())->values(),
        );
        static::assertNotSame(
            row_hash(ref('a'))->eval($original, new PhpBackend())->values(),
            row_hash(ref('c'))->eval($renamed, new PhpBackend())->values(),
        );
    }

    public function test_a_reference_the_batch_lacks_is_refused_with_a_suggestion(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Schema definition for column "emal" not found. Did you mean one of: [email]?');

        row_hash(ref('a'), ref('emal'))->eval(
            array_to_rows([['a' => 1, 'email' => 'x']], schema(int_schema('a'), str_schema('email'))),
            new PhpBackend(),
        );
    }

    public function test_a_batch_without_rows_needs_no_referenced_column(): void
    {
        static::assertSame(
            [],
            row_hash(ref('missing'))->eval(Rows::fromColumns(schema(), [], 0), new PhpBackend())->values(),
        );
    }

    public function test_a_null_value_hashes_like_a_row_without_that_column(): void
    {
        static::assertSame(
            ['8e18789eac90baf2a1f6bc7cceafa11a'],
            row_hash(ref('a'), ref('b'))
                ->eval(
                    array_to_rows([['a' => 1, 'b' => null]], schema(int_schema('a'), str_schema('b', true))),
                    new PhpBackend(),
                )
                ->values(),
        );
        static::assertSame(
            ['8e18789eac90baf2a1f6bc7cceafa11a'],
            row_hash(ref('a'))->eval(array_to_rows([['a' => 1]], schema(int_schema('a'))), new PhpBackend())->values(),
        );
    }

    public function test_it_returns_a_not_null_string(): void
    {
        static::assertEquals(type_string(), row_hash(ref('a'))->returns());
    }

    public function test_it_needs_at_least_one_reference(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('row_hash() needs at least one column reference');

        new RowHash([]);
    }

    public function test_its_children_are_its_references_and_a_rebuilt_one_hashes_alike(): void
    {
        $function = row_hash(ref('a'), ref('b'));
        $rows = array_to_rows([['a' => 1, 'b' => 'x']], schema(int_schema('a'), str_schema('b')));

        static::assertEquals([ref('a'), ref('b')], $function->children());
        static::assertSame(
            $function->eval($rows, new PhpBackend())->values(),
            $function
                ->withChildren([ref('b'), ref('a')])
                ->eval($rows, new PhpBackend())
                ->values(),
        );
        static::assertSame(
            $function->eval($rows, new PhpBackend())->values(),
            DescribeRoundTrip::rebuilt($function)->eval($rows, new PhpBackend())->values(),
        );
    }
}
