<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Column\Php;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Tests\Mother\ColumnMother;
use Flow\ETL\Values\Column\MultisetDigest;
use Flow\ETL\Values\Column\Php\PhpGroupedMultisetHashes;
use Flow\ETL\Values\Column\PhpBackend;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function hash;
use function unpack;

final class PhpGroupedMultisetHashesTest extends TestCase
{
    public function test_each_row_is_summed_into_its_group(): void
    {
        $hashes = new PhpGroupedMultisetHashes(HashAlgorithm::xxh128);
        $hashes->add(
            array_to_rows([['a' => 1], ['a' => 2], ['a' => 1]], schema(int_schema('a')))->columns(),
            ColumnMother::of(int_schema('id'), [0, 1, 0]),
        );

        static::assertSame(
            ['1c30f13d592175e443ed78f99d5f4234', 'e406cdb168be00960a58088d91a960c5'],
            $hashes->hashes()->physicals(),
        );
    }

    #[TestWith([HashAlgorithm::md5, '53b117ed69552c4ee4c638e6c72a526a', '8c589f902ff9330489f46e8138ef3fff'])]
    #[TestWith([HashAlgorithm::sha256, '8f55cd2c66338b181c9f478cff77d05c', '7c396438b2b1506bc82ea94d4ad238cd'])]
    public function test_another_algorithm_sums_the_first_128_bits_of_its_digest(
        HashAlgorithm $algorithm,
        string $first,
        string $second,
    ): void {
        $hashes = new PhpGroupedMultisetHashes($algorithm);
        $hashes->add(
            array_to_rows([['a' => 1], ['a' => 2], ['a' => 1]], schema(int_schema('a')))->columns(),
            ColumnMother::of(int_schema('id'), [0, 1, 0]),
        );

        static::assertSame([$first, $second], $hashes->hashes()->physicals());
    }

    public function test_a_group_below_the_highest_seen_one_is_zero(): void
    {
        $hashes = new PhpGroupedMultisetHashes(HashAlgorithm::xxh128);
        $hashes->add(
            array_to_rows([['a' => 1]], schema(int_schema('a')))->columns(),
            ColumnMother::of(int_schema('id'), [1]),
        );

        static::assertSame(
            ['00000000000000000000000000000000', '8e18789eac90baf2a1f6bc7cceafa11a'],
            $hashes->hashes()->physicals(),
        );
    }

    public function test_no_rows_give_no_groups(): void
    {
        $hashes = new PhpGroupedMultisetHashes(HashAlgorithm::xxh128);
        $hashes->add([], ColumnMother::of(int_schema('id'), []));

        static::assertSame([], $hashes->hashes()->physicals());
    }

    public function test_hashes_are_the_digests_by_group(): void
    {
        $hashes = new PhpGroupedMultisetHashes(HashAlgorithm::xxh128);
        $hashes->add(
            array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a')))->columns(),
            ColumnMother::of(int_schema('id'), [0, 1]),
        );

        static::assertSame(
            ['8e18789eac90baf2a1f6bc7cceafa11a', 'e406cdb168be00960a58088d91a960c5'],
            $hashes->hashes()->physicals(),
        );
    }

    public function test_earlier_groups_are_added_to_the_groups_they_map_to(): void
    {
        $earlier = new PhpGroupedMultisetHashes(HashAlgorithm::xxh128);
        $earlier->add(
            array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a')))->columns(),
            ColumnMother::of(int_schema('id'), [0, 1]),
        );
        $later = new PhpGroupedMultisetHashes(HashAlgorithm::xxh128);
        $later->add(
            array_to_rows([['a' => 1]], schema(int_schema('a')))->columns(),
            ColumnMother::of(int_schema('id'), [0]),
        );
        $later->absorbEarlier($earlier, [1 => 0, 0 => 2]);

        static::assertSame(
            [
                '721f4650154ebb88ac4ec50a605901df',
                '00000000000000000000000000000000',
                '8e18789eac90baf2a1f6bc7cceafa11a',
            ],
            $later->hashes()->physicals(),
        );
    }

    public function test_a_batch_without_columns_hashes_each_row_as_the_empty_row(): void
    {
        /** @var array{1: int, 2: int} $lanes */
        $lanes = unpack('J2', hash('xxh128', '', true));
        $empty = new MultisetDigest($lanes[2], $lanes[1]);
        $hashes = new PhpGroupedMultisetHashes(HashAlgorithm::xxh128);
        $hashes->add([], ColumnMother::of(int_schema('id'), [1, 0, 1]));

        static::assertSame([$empty->hex(), $empty->plus($empty)->hex()], $hashes->hashes()->physicals());
    }

    public function test_one_row_in_one_group_equals_its_row_hash(): void
    {
        $rows = array_to_rows([['a' => 1]], schema(int_schema('a')));
        $hashes = new PhpGroupedMultisetHashes(HashAlgorithm::xxh128);
        $hashes->add($rows->columns(), ColumnMother::of(int_schema('id'), [0]));

        static::assertSame(['8e18789eac90baf2a1f6bc7cceafa11a'], $hashes->hashes()->physicals());
        static::assertSame(
            (new PhpBackend())
                ->rowHashes($rows->columns(), 1)
                ->physicals(),
            $hashes->hashes()->physicals(),
        );
    }

    public function test_row_order_over_batches_does_not_count(): void
    {
        $forward = new PhpGroupedMultisetHashes(HashAlgorithm::xxh128);
        $forward->add(
            array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a')))->columns(),
            ColumnMother::of(int_schema('id'), [0, 0]),
        );
        $backward = new PhpGroupedMultisetHashes(HashAlgorithm::xxh128);
        $backward->add(
            array_to_rows([['a' => 2]], schema(int_schema('a')))->columns(),
            ColumnMother::of(int_schema('id'), [0]),
        );
        $backward->add(
            array_to_rows([['a' => 1]], schema(int_schema('a')))->columns(),
            ColumnMother::of(int_schema('id'), [0]),
        );

        static::assertSame(['721f4650154ebb88ac4ec50a605901df'], $forward->hashes()->physicals());
        static::assertSame($forward->hashes()->physicals(), $backward->hashes()->physicals());
    }

    public function test_a_row_of_only_nulls_counts_as_a_row(): void
    {
        $hashes = new PhpGroupedMultisetHashes(HashAlgorithm::xxh128);
        $hashes->add(
            array_to_rows([['a' => null]], schema(int_schema('a', true)))->columns(),
            ColumnMother::of(int_schema('id'), [0]),
        );

        static::assertSame(['99aa06d3014798d86001c324468d497f'], $hashes->hashes()->physicals());
    }

    public function test_an_earlier_group_it_lacks_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Earlier group 3 has no hash');

        (new PhpGroupedMultisetHashes(HashAlgorithm::xxh128))->absorbEarlier(
            new PhpGroupedMultisetHashes(HashAlgorithm::xxh128),
            [3 => 0],
        );
    }
}
