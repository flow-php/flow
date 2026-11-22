<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use Flow\ETL\Runtime\Executor\WholeOutputHasher;
use Flow\ETL\Runtime\Metrics\DatasetHash;
use Flow\ETL\Runtime\Metrics\Hash;
use Flow\ETL\Runtime\Metrics\PartitionHash;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Mother\ColumnMother;
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
        $kernel = (new PhpBackend())->groupedMultisetHashes();
        $kernel->add($rows->columns(), ColumnMother::of(int_schema('id'), [0, 0]));
        $hasher = new WholeOutputHasher();
        $hasher->add(array_to_rows([['a' => 2]], schema(int_schema('a'))), new PhpBackend());
        $hasher->add(array_to_rows([['a' => 1]], schema(int_schema('a'))), new PhpBackend());

        static::assertEquals(new DatasetHash(new Hash(2, '721f4650154ebb88ac4ec50a605901df'), []), $hasher->hash());
        static::assertSame([$hasher->hash()->total->value], $kernel->hashes()->physicals());
    }

    public function test_absorb_reads_only_the_total_of_a_partial(): void
    {
        $hasher = new WholeOutputHasher();
        $hasher->add(array_to_rows([['a' => 1]], schema(int_schema('a'))), new PhpBackend());
        $hasher->absorb(new DatasetHash(new Hash(1, 'e406cdb168be00960a58088d91a960c5'), [
            new PartitionHash(new Partitions(new Partition('a', '2')), new Hash(5, '00000000000000000000000000000001')),
        ]));

        static::assertEquals(new DatasetHash(new Hash(2, '721f4650154ebb88ac4ec50a605901df'), []), $hasher->hash());
    }

    public function test_a_hasher_that_saw_nothing_gives_the_empty_digest(): void
    {
        static::assertEquals(
            new DatasetHash(new Hash(0, '00000000000000000000000000000000'), []),
            (new WholeOutputHasher())->hash(),
        );
    }

    public function test_a_hasher_whose_kernel_saw_no_row_gives_the_empty_digest(): void
    {
        $hasher = new WholeOutputHasher();
        $hasher->add(array_to_rows([], schema(int_schema('a'))), new PhpBackend());

        static::assertEquals(new DatasetHash(new Hash(0, '00000000000000000000000000000000'), []), $hasher->hash());
    }

    public function test_describe_writes_no_state(): void
    {
        $hasher = new WholeOutputHasher();
        $hasher->add(array_to_rows([['a' => 1]], schema(int_schema('a'))), new PhpBackend());

        static::assertEquals(
            new DatasetHash(new Hash(0, '00000000000000000000000000000000'), []),
            DescribeRoundTrip::rebuilt($hasher)->hash(),
        );
    }
}
