<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\MultisetDigest;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\multiset_hash;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row_hash;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class MultisetHashTest extends FlowTestCase
{
    public function test_the_value_is_the_wrapping_lane_sum_of_the_row_hashes(): void
    {
        $backend = new PhpBackend();
        $rows = array_to_rows(
            [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'y']],
            schema(int_schema('a'), str_schema('b')),
        );
        $rowHashes = type_list(type_string())->assert(row_hash(ref('a'), ref('b'))->eval($rows, $backend)->values());
        $aggregate = multiset_hash();
        $aggregate->aggregate($rows, [0, 1], $backend);
        $summed = multiset_hash();

        foreach ($rowHashes as $rowHash) {
            $summed->merge(['hash' => $rowHash], $backend);
        }

        static::assertSame(['bdea5ee4847f3df6cb724988fb66ecd4', 'ba7c3390ce5fcedfea5c7d697dbe1ac9'], $rowHashes);
        static::assertSame('7866927552df0cd5b5cec6f27925079d', $aggregate->value());
        static::assertSame($aggregate->value(), $summed->value());
    }

    public function test_the_row_order_and_the_batches_do_not_count(): void
    {
        $backend = new PhpBackend();
        $schema = schema(int_schema('a'), str_schema('b'));
        $forward = multiset_hash();
        $forward->aggregate(array_to_rows([['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'y']], $schema), [0, 1], $backend);
        $split = multiset_hash();
        $split->aggregate(array_to_rows([['a' => 2, 'b' => 'y']], $schema), [0], $backend);
        $split->aggregate(array_to_rows([['a' => 1, 'b' => 'x']], $schema), [0], $backend);

        static::assertSame('7866927552df0cd5b5cec6f27925079d', $split->value());
        static::assertSame($forward->value(), $split->value());
    }

    public function test_a_duplicate_row_counts(): void
    {
        $aggregate = multiset_hash();
        $aggregate->aggregate(
            array_to_rows([['a' => 1, 'b' => 'x'], ['a' => 1, 'b' => 'x']], schema(int_schema('a'), str_schema('b'))),
            [0, 1],
            new PhpBackend(),
        );

        static::assertSame('7bd4bdc908fe7bec96e49311f6cdd9a8', $aggregate->value());
    }

    public function test_only_the_indexed_rows_are_hashed(): void
    {
        $aggregate = multiset_hash();
        $aggregate->aggregate(
            array_to_rows(
                [['a' => 1, 'b' => 'x'], ['a' => 9, 'b' => 'z'], ['a' => 2, 'b' => 'y']],
                schema(int_schema('a'), str_schema('b')),
            ),
            [0, 2],
            new PhpBackend(),
        );

        static::assertSame('7866927552df0cd5b5cec6f27925079d', $aggregate->value());
    }

    public function test_references_name_the_hashed_columns(): void
    {
        $aggregate = multiset_hash(ref('a'));
        $aggregate->aggregate(
            array_to_rows([['a' => 1, 'b' => 'x']], schema(int_schema('a'), str_schema('b'))),
            [0],
            new PhpBackend(),
        );

        static::assertSame('8e18789eac90baf2a1f6bc7cceafa11a', $aggregate->value());
        static::assertEquals([ref('a')], $aggregate->references());
        static::assertNull(multiset_hash()->references());
    }

    public function test_merging_two_partials_equals_one_pass(): void
    {
        $backend = new PhpBackend();
        $schema = schema(int_schema('a'), str_schema('b'));
        $first = multiset_hash();
        $first->aggregate(array_to_rows([['a' => 1, 'b' => 'x']], $schema), [0], $backend);
        $second = multiset_hash();
        $second->aggregate(array_to_rows([['a' => 2, 'b' => 'y']], $schema), [0], $backend);
        $merged = multiset_hash();
        $merged->merge($first->partial(), $backend);
        $merged->merge($second->partial(), $backend);

        static::assertSame('7866927552df0cd5b5cec6f27925079d', $merged->value());
        static::assertSame(['hash' => '7866927552df0cd5b5cec6f27925079d'], $merged->partial());
    }

    public function test_an_aggregate_that_saw_no_row_is_zero(): void
    {
        static::assertSame('00000000000000000000000000000000', multiset_hash()->value());
        static::assertSame(['hash' => '00000000000000000000000000000000'], multiset_hash()->partial());
    }

    public function test_it_declares_its_output_and_partial(): void
    {
        static::assertSame('_multiset_hash', multiset_hash()->outputName());
        static::assertEquals(type_string(), multiset_hash()->returns());
        static::assertEquals(type_structure(['hash' => type_string()]), multiset_hash()->partialType());
    }

    public function test_a_rebuilt_aggregate_keeps_its_references(): void
    {
        static::assertEquals(
            [ref('a'), ref('b')],
            DescribeRoundTrip::rebuilt(multiset_hash(ref('a'), ref('b')))->references(),
        );
        static::assertNull(DescribeRoundTrip::rebuilt(multiset_hash())->references());
        static::assertEquals([ref('b')], multiset_hash(ref('a'))->withChildren([ref('b')])->children());
        static::assertTrue(multiset_hash(ref('a'))->deterministic());
        static::assertEquals([], multiset_hash()->withChildren([])->children());
        static::assertTrue(multiset_hash()->resolved());
    }

    public function test_with_children_refuses_another_number_of_children(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'multiset_hash() has 1 reference children; withChildren() must receive them back.',
        );

        multiset_hash(ref('a'))->withChildren([]);
    }

    public function test_a_named_column_the_batch_lacks_is_refused_with_a_suggestion(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Schema definition for column "emal" not found. Did you mean one of: [email]?');

        multiset_hash(ref('emal'))
            ->aggregate(array_to_rows([['email' => 'x']], schema(str_schema('email'))), [0], new PhpBackend());
    }

    public function test_a_batch_without_rows_needs_no_named_column(): void
    {
        $aggregate = multiset_hash(ref('missing'));
        $aggregate->aggregate(array_to_rows([], schema(int_schema('a'))), [], new PhpBackend());

        static::assertSame(MultisetDigest::empty()->hex(), $aggregate->value());
    }

    public function test_without_references_a_null_column_hashes_like_a_batch_without_that_column(): void
    {
        $withNulls = multiset_hash();
        $withNulls->aggregate(
            array_to_rows([['a' => 1, 'b' => null]], schema(int_schema('a'), str_schema('b', true))),
            [0],
            new PhpBackend(),
        );
        $without = multiset_hash();
        $without->aggregate(array_to_rows([['a' => 1]], schema(int_schema('a'))), [0], new PhpBackend());

        static::assertSame('8e18789eac90baf2a1f6bc7cceafa11a', $withNulls->value());
        static::assertSame($without->value(), $withNulls->value());
    }
}
