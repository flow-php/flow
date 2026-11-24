<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Metrics;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Runtime\Metrics\DatasetHash;
use Flow\ETL\Runtime\Metrics\Hash;
use Flow\ETL\Runtime\Metrics\PartitionHash;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use PHPUnit\Framework\TestCase;

final class DatasetHashTest extends TestCase
{
    public function test_it_gives_the_total_and_the_partitions(): void
    {
        $partition = new PartitionHash(
            new Partitions(new Partition('country', 'PL')),
            new Hash(2, 'ab8476f29c95212a306bffff897d7afb'),
        );
        $hash = new DatasetHash(new Hash(3, '31be1d609a26d996382e2a23855ebbe5'), [$partition], HashAlgorithm::xxh128);

        static::assertEquals(new Hash(3, '31be1d609a26d996382e2a23855ebbe5'), $hash->total);
        static::assertSame([$partition], $hash->partitions);
    }

    public function test_plus_adds_the_totals_and_the_hashes_of_equal_partitions(): void
    {
        static::assertEquals(
            new DatasetHash(
                new Hash(3, '00000000000000000000000000000003'),
                [
                    new PartitionHash(
                        new Partitions(new Partition('country', 'PL')),
                        new Hash(3, '00000000000000000000000000000003'),
                    ),
                ],
                HashAlgorithm::xxh128,
            ),
            (new DatasetHash(
                new Hash(1, '00000000000000000000000000000001'),
                [
                    new PartitionHash(
                        new Partitions(new Partition('country', 'PL')),
                        new Hash(1, '00000000000000000000000000000001'),
                    ),
                ],
                HashAlgorithm::xxh128,
            ))->plus(
                new DatasetHash(
                    new Hash(2, '00000000000000000000000000000002'),
                    [
                        new PartitionHash(
                            new Partitions(new Partition('country', 'PL')),
                            new Hash(2, '00000000000000000000000000000002'),
                        ),
                    ],
                    HashAlgorithm::xxh128,
                ),
            ),
        );
    }

    public function test_plus_keeps_different_partitions_in_order_of_first_appearance(): void
    {
        $pl = new PartitionHash(
            new Partitions(new Partition('country', 'PL')),
            new Hash(1, '00000000000000000000000000000001'),
        );
        $de = new PartitionHash(
            new Partitions(new Partition('country', 'DE')),
            new Hash(1, '00000000000000000000000000000002'),
        );
        $us = new PartitionHash(
            new Partitions(new Partition('country', 'US')),
            new Hash(1, '00000000000000000000000000000004'),
        );

        static::assertEquals(
            [$pl, $de, $us],
            (new DatasetHash(new Hash(2, '00000000000000000000000000000003'), [$pl, $de], HashAlgorithm::xxh128))->plus(
                new DatasetHash(new Hash(1, '00000000000000000000000000000004'), [$us], HashAlgorithm::xxh128),
            )->partitions,
        );
    }

    public function test_plus_merges_equal_partitions_of_one_operand(): void
    {
        static::assertEquals(
            [new PartitionHash(
                new Partitions(new Partition('country', 'PL')),
                new Hash(2, '00000000000000000000000000000003'),
            )],
            (new DatasetHash(
                new Hash(2, '00000000000000000000000000000003'),
                [
                    new PartitionHash(
                        new Partitions(new Partition('country', 'PL')),
                        new Hash(1, '00000000000000000000000000000001'),
                    ),
                    new PartitionHash(
                        new Partitions(new Partition('country', 'PL')),
                        new Hash(1, '00000000000000000000000000000002'),
                    ),
                ],
                HashAlgorithm::xxh128,
            ))->plus(new DatasetHash(Hash::empty(), [], HashAlgorithm::xxh128))->partitions,
        );
    }

    public function test_plus_takes_null_and_the_null_folder_name_as_one_partition(): void
    {
        static::assertEquals(
            [new PartitionHash(
                new Partitions(new Partition('country', null)),
                new Hash(2, '00000000000000000000000000000003'),
            )],
            (new DatasetHash(
                new Hash(1, '00000000000000000000000000000001'),
                [
                    new PartitionHash(
                        new Partitions(new Partition('country', null)),
                        new Hash(1, '00000000000000000000000000000001'),
                    ),
                ],
                HashAlgorithm::xxh128,
            ))->plus(
                new DatasetHash(
                    new Hash(1, '00000000000000000000000000000002'),
                    [
                        new PartitionHash(
                            new Partitions(new Partition('country', Partition::NULL_VALUE)),
                            new Hash(1, '00000000000000000000000000000002'),
                        ),
                    ],
                    HashAlgorithm::xxh128,
                ),
            )->partitions,
        );
    }

    public function test_plus_with_the_empty_hash_is_the_identity(): void
    {
        $hash = new DatasetHash(
            new Hash(1, '00000000000000000000000000000001'),
            [
                new PartitionHash(
                    new Partitions(new Partition('country', 'PL')),
                    new Hash(1, '00000000000000000000000000000001'),
                ),
            ],
            HashAlgorithm::xxh128,
        );

        static::assertEquals($hash, $hash->plus(new DatasetHash(Hash::empty(), [], HashAlgorithm::xxh128)));
        static::assertEquals($hash, (new DatasetHash(Hash::empty(), [], HashAlgorithm::xxh128))->plus($hash));
    }

    public function test_plus_of_two_whole_hashes_adds_the_totals_and_has_no_partitions(): void
    {
        static::assertEquals(
            new DatasetHash(new Hash(5, 'ffffffffffffffff0000000000000000'), [], HashAlgorithm::xxh128),
            (new DatasetHash(new Hash(2, 'ffffffffffffffffffffffffffffffff'), [], HashAlgorithm::xxh128))->plus(
                new DatasetHash(new Hash(3, '00000000000000000000000000000001'), [], HashAlgorithm::xxh128),
            ),
        );
    }

    public function test_normalize_writes_the_total_and_each_partition(): void
    {
        static::assertSame(
            [
                'algorithm' => 'xxh128',
                'total' => ['rows' => 3, 'value' => '31be1d609a26d996382e2a23855ebbe5'],
                'partitions' => [
                    [
                        'partitions' => ['country' => 'PL'],
                        'hash' => ['rows' => 2, 'value' => 'ab8476f29c95212a306bffff897d7afb'],
                    ],
                    [
                        'partitions' => ['country' => null],
                        'hash' => ['rows' => 1, 'value' => 'f3aebd664d56cdeceed04ea04775ad52'],
                    ],
                ],
            ],
            (new DatasetHash(
                new Hash(3, '31be1d609a26d996382e2a23855ebbe5'),
                [
                    new PartitionHash(
                        new Partitions(new Partition('country', 'PL')),
                        new Hash(2, 'ab8476f29c95212a306bffff897d7afb'),
                    ),
                    new PartitionHash(
                        new Partitions(new Partition('country', null)),
                        new Hash(1, 'f3aebd664d56cdeceed04ea04775ad52'),
                    ),
                ],
                HashAlgorithm::xxh128,
            ))->normalize(),
        );
    }

    public function test_from_array_reads_back_what_normalize_wrote_with_a_null_partition_value(): void
    {
        $hash = new DatasetHash(
            new Hash(2, '31be1d609a26d996382e2a23855ebbe5'),
            [
                new PartitionHash(
                    new Partitions(new Partition('date', '2026-11-20'), new Partition('country', null)),
                    new Hash(2, 'ab8476f29c95212a306bffff897d7afb'),
                ),
            ],
            HashAlgorithm::xxh128,
        );

        static::assertEquals($hash, DatasetHash::fromArray($hash->normalize()));
    }

    public function test_a_hash_without_partitions_reads_back_without_partitions(): void
    {
        $hash = new DatasetHash(new Hash(3, '31be1d609a26d996382e2a23855ebbe5'), [], HashAlgorithm::xxh128);

        static::assertSame(
            [
                'algorithm' => 'xxh128',
                'total' => ['rows' => 3, 'value' => '31be1d609a26d996382e2a23855ebbe5'],
                'partitions' => [],
            ],
            $hash->normalize(),
        );
        static::assertEquals($hash, DatasetHash::fromArray($hash->normalize()));
    }

    public function test_a_stored_hash_keeps_its_algorithm(): void
    {
        $stored = DatasetHash::fromArray([
            'algorithm' => 'sha256',
            'total' => ['rows' => 1, 'value' => '00000000000000000000000000000001'],
            'partitions' => [],
        ]);

        static::assertSame(HashAlgorithm::sha256, $stored->algorithm);
        static::assertSame(HashAlgorithm::sha256, $stored->plus($stored)->algorithm);
    }

    public function test_from_array_needs_the_algorithm(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A dataset hash needs "algorithm" of type string');

        DatasetHash::fromArray(['total' => ['rows' => 0, 'value' => ''], 'partitions' => []]);
    }

    public function test_from_array_refuses_an_unknown_algorithm(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown hash algorithm "crc32", use one of: md5, sha256, xxh128');

        DatasetHash::fromArray([
            'algorithm' => 'crc32',
            'total' => ['rows' => 0, 'value' => '00000000000000000000000000000000'],
            'partitions' => [],
        ]);
    }

    public function test_plus_refuses_a_hash_of_another_algorithm(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'A sha256 dataset hash cannot be added to a xxh128 dataset hash, hash both datasets with the same algorithm',
        );

        (new DatasetHash(Hash::empty(), [], HashAlgorithm::xxh128))->plus(
            new DatasetHash(Hash::empty(), [], HashAlgorithm::sha256),
        );
    }

    public function test_from_array_names_a_missing_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A dataset hash needs "total" of type array<mixed>');

        DatasetHash::fromArray(['algorithm' => 'xxh128', 'partitions' => []]);
    }

    public function test_from_array_names_a_key_of_another_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A dataset hash needs "partitions" of type list<array<mixed>>');

        DatasetHash::fromArray([
            'algorithm' => 'xxh128',
            'total' => ['rows' => 3, 'value' => '31be1d609a26d996382e2a23855ebbe5'],
            'partitions' => 'none',
        ]);
    }

    public function test_from_array_names_a_missing_key_of_the_total(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A hash needs "value" of type string');

        DatasetHash::fromArray(['algorithm' => 'xxh128', 'total' => ['rows' => 3], 'partitions' => []]);
    }
}
