<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Metrics;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Runtime\Metrics\Hash;
use Flow\ETL\Runtime\Metrics\PartitionHash;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use PHPUnit\Framework\TestCase;

final class PartitionHashTest extends TestCase
{
    public function test_normalize_writes_the_partitions_in_declaration_order_with_a_null_value(): void
    {
        static::assertSame(
            [
                'partitions' => ['date' => '2026-11-20', 'country' => null],
                'hash' => ['rows' => 120, 'value' => 'ab8476f29c95212a306bffff897d7afb'],
            ],
            (new PartitionHash(
                new Partitions(new Partition('date', '2026-11-20'), new Partition('country', null)),
                new Hash(120, 'ab8476f29c95212a306bffff897d7afb'),
            ))->normalize(),
        );
    }

    public function test_from_array_reads_back_what_normalize_wrote(): void
    {
        $hash = new PartitionHash(
            new Partitions(new Partition('date', '2026-11-20'), new Partition('country', null)),
            new Hash(120, 'ab8476f29c95212a306bffff897d7afb'),
        );

        static::assertEquals($hash, PartitionHash::fromArray($hash->normalize()));
    }

    public function test_from_array_names_a_missing_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A partition hash needs "hash" of type array<mixed>');

        PartitionHash::fromArray(['partitions' => ['date' => '2026-11-20']]);
    }

    public function test_from_array_names_a_key_of_another_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A partition hash needs "partitions" of type map<string, ?string>');

        PartitionHash::fromArray(['partitions' => ['date' => 1], 'hash' => ['rows' => 1, 'value' => 'ab']]);
    }

    public function test_from_array_names_a_missing_key_of_the_hash(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A hash needs "rows" of type integer');

        PartitionHash::fromArray(['partitions' => ['date' => '2026-11-20'], 'hash' => ['value' => 'ab']]);
    }
}
