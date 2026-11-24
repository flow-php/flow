<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Functions\CountsByGroup;
use Flow\ETL\Functions\MultisetHashesByGroup;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class MultisetHashesByGroupTest extends FlowTestCase
{
    public function test_each_group_hashes_its_rows_with_every_column_of_the_batch(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('g'), int_schema('v'));
        $rows = array_to_rows([['g' => 'a', 'v' => 1], ['g' => 'a', 'v' => 2], ['g' => 'b', 'v' => 1]], $schema);
        $hashes = new MultisetHashesByGroup([], $backend->groupedMultisetHashes(HashAlgorithm::xxh128));

        $hashes->aggregateGroups($rows, $backend->groupTable(['g'], $schema)->assign($rows), $backend);

        static::assertSame(
            ['1a61bf177bb5a2b671c0494779768cc9', '5df1c2802504f984036241b9f1e54ea0'],
            $hashes->groupValues(2, $backend)->values(),
        );
    }

    public function test_the_row_order_and_the_batches_do_not_count(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('g'), int_schema('v'));
        $table = $backend->groupTable(['g'], $schema);
        $hashes = new MultisetHashesByGroup([], $backend->groupedMultisetHashes(HashAlgorithm::xxh128));
        $first = array_to_rows([['g' => 'a', 'v' => 2], ['g' => 'b', 'v' => 1]], $schema);
        $second = array_to_rows([['g' => 'a', 'v' => 1]], $schema);

        $hashes->aggregateGroups($first, $table->assign($first), $backend);
        $hashes->aggregateGroups($second, $table->assign($second), $backend);

        static::assertSame(
            ['1a61bf177bb5a2b671c0494779768cc9', '5df1c2802504f984036241b9f1e54ea0'],
            $hashes->groupValues(2, $backend)->values(),
        );
    }

    public function test_named_columns_hash_only_those_columns(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('g'), int_schema('v'));
        $rows = array_to_rows([['g' => 'a', 'v' => 1], ['g' => 'a', 'v' => 1]], $schema);
        $hashes = new MultisetHashesByGroup(['v'], $backend->groupedMultisetHashes(HashAlgorithm::xxh128));

        $hashes->aggregateGroups($rows, $backend->groupTable(['g'], $schema)->assign($rows), $backend);

        static::assertSame(['e0191505b9cce3349c6f8093cab56dec'], $hashes->groupValues(1, $backend)->values());
        static::assertSame([['hash' => 'e0191505b9cce3349c6f8093cab56dec']], $hashes->partials(1));
    }

    public function test_earlier_groups_are_absorbed_into_the_groups_they_map_to(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('g'), int_schema('v'));
        $earlierRows = array_to_rows([['g' => 'a', 'v' => 1], ['g' => 'b', 'v' => 1]], $schema);
        $laterRows = array_to_rows([['g' => 'b', 'v' => 9], ['g' => 'a', 'v' => 2]], $schema);
        $earlier = new MultisetHashesByGroup([], $backend->groupedMultisetHashes(HashAlgorithm::xxh128));
        $later = new MultisetHashesByGroup([], $backend->groupedMultisetHashes(HashAlgorithm::xxh128));

        $earlier->aggregateGroups($earlierRows, $backend->groupTable(['g'], $schema)->assign($earlierRows), $backend);
        $later->aggregateGroups($laterRows, $backend->groupTable(['g'], $schema)->assign($laterRows), $backend);
        $later->absorbEarlier($earlier, [0 => 1, 1 => 0], $backend);
        $later->absorbEarlier($earlier, [], $backend);

        static::assertSame('1a61bf177bb5a2b671c0494779768cc9', $later->groupValues(2, $backend)->values()[1]);
    }

    public function test_it_absorbs_only_grouped_multiset_hashes(): void
    {
        $backend = new PhpBackend();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Grouped multiset hashes absorb only other grouped multiset hashes');

        (new MultisetHashesByGroup([], $backend->groupedMultisetHashes(HashAlgorithm::xxh128)))->absorbEarlier(
            new CountsByGroup(null, $backend->groupedCounts()),
            [0 => 0],
            $backend,
        );
    }

    public function test_a_named_column_the_batch_lacks_is_refused_with_a_suggestion(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('g'), int_schema('value'));
        $rows = array_to_rows([['g' => 'a', 'value' => 1]], $schema);
        $hashes = new MultisetHashesByGroup(['valu'], $backend->groupedMultisetHashes(HashAlgorithm::xxh128));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Schema definition for column "valu" not found. Did you mean one of: [value]?');

        $hashes->aggregateGroups($rows, $backend->groupTable(['g'], $schema)->assign($rows), $backend);
    }

    public function test_a_batch_without_rows_needs_no_named_column(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('g'));
        $rows = array_to_rows([], $schema);
        $hashes = new MultisetHashesByGroup(['missing'], $backend->groupedMultisetHashes(HashAlgorithm::xxh128));

        $hashes->aggregateGroups($rows, $backend->groupTable(['g'], $schema)->assign($rows), $backend);

        static::assertSame([], $hashes->groupValues(0, $backend)->values());
    }
}
