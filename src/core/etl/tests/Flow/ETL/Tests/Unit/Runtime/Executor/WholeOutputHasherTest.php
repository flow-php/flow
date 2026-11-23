<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Runtime\Executor\WholeOutputHasher;
use Flow\ETL\Runtime\Metrics\DatasetHash;
use Flow\ETL\Runtime\Metrics\Hash;
use Flow\ETL\Runtime\Metrics\PartitionHash;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Mother\ColumnMother;
use Flow\ETL\Values\Column\MultisetHashAlgorithm;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class WholeOutputHasherTest extends TestCase
{
    public function test_it_gives_the_hash_of_all_rows_and_no_partitions(): void
    {
        $rows = array_to_rows([['a' => 1], ['a' => 2]], schema(int_schema('a')));
        $kernel = (new PhpBackend())->groupedMultisetHashes(MultisetHashAlgorithm::xxh128);
        $kernel->add($rows->columns(), ColumnMother::of(int_schema('id'), [0, 0]));
        $hasher = new WholeOutputHasher(MultisetHashAlgorithm::xxh128);
        $hasher->add(array_to_rows([['a' => 2]], schema(int_schema('a'))), new PhpBackend());
        $hasher->add(array_to_rows([['a' => 1]], schema(int_schema('a'))), new PhpBackend());

        static::assertEquals(
            new DatasetHash(new Hash(2, '721f4650154ebb88ac4ec50a605901df'), [], MultisetHashAlgorithm::xxh128),
            $hasher->hash(),
        );
        static::assertSame([$hasher->hash()->total->value], $kernel->hashes()->physicals());
    }

    public function test_another_algorithm_is_hashed_reported_and_described(): void
    {
        $hasher = new WholeOutputHasher(MultisetHashAlgorithm::sha256);
        $hasher->add(array_to_rows([['a' => 2]], schema(int_schema('a'))), new PhpBackend());

        static::assertEquals(
            new DatasetHash(new Hash(1, '7c396438b2b1506bc82ea94d4ad238cd'), [], MultisetHashAlgorithm::sha256),
            $hasher->hash(),
        );
        static::assertSame(['algorithm' => 'sha256'], $hasher->describe()?->data);
    }

    public function test_absorb_refuses_a_partial_of_another_algorithm(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A md5 dataset hash cannot be added to a xxh128 dataset hash');

        (new WholeOutputHasher(MultisetHashAlgorithm::xxh128))->absorb(
            new DatasetHash(Hash::empty(), [], MultisetHashAlgorithm::md5),
        );
    }

    public function test_absorb_reads_only_the_total_of_a_partial(): void
    {
        $hasher = new WholeOutputHasher(MultisetHashAlgorithm::xxh128);
        $hasher->add(array_to_rows([['a' => 1]], schema(int_schema('a'))), new PhpBackend());
        $hasher->absorb(
            new DatasetHash(
                new Hash(1, 'e406cdb168be00960a58088d91a960c5'),
                [
                    new PartitionHash(
                        new Partitions(new Partition('a', '2')),
                        new Hash(5, '00000000000000000000000000000001'),
                    ),
                ],
                MultisetHashAlgorithm::xxh128,
            ),
        );

        static::assertEquals(
            new DatasetHash(new Hash(2, '721f4650154ebb88ac4ec50a605901df'), [], MultisetHashAlgorithm::xxh128),
            $hasher->hash(),
        );
    }

    public function test_a_hasher_that_saw_nothing_gives_the_empty_digest(): void
    {
        static::assertEquals(
            new DatasetHash(new Hash(0, '00000000000000000000000000000000'), [], MultisetHashAlgorithm::xxh128),
            (new WholeOutputHasher(MultisetHashAlgorithm::xxh128))->hash(),
        );
    }

    public function test_a_hasher_whose_kernel_saw_no_row_gives_the_empty_digest(): void
    {
        $hasher = new WholeOutputHasher(MultisetHashAlgorithm::xxh128);
        $hasher->add(array_to_rows([], schema(int_schema('a'))), new PhpBackend());

        static::assertEquals(
            new DatasetHash(new Hash(0, '00000000000000000000000000000000'), [], MultisetHashAlgorithm::xxh128),
            $hasher->hash(),
        );
    }

    public function test_describe_writes_no_state(): void
    {
        $hasher = new WholeOutputHasher(MultisetHashAlgorithm::xxh128);
        $hasher->add(array_to_rows([['a' => 1]], schema(int_schema('a'))), new PhpBackend());

        static::assertEquals(
            new DatasetHash(new Hash(0, '00000000000000000000000000000000'), [], MultisetHashAlgorithm::xxh128),
            DescribeRoundTrip::rebuilt($hasher)->hash(),
        );
    }
}
