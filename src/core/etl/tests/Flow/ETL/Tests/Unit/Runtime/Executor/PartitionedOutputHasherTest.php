<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Runtime\Executor\PartitionedOutputHasher;
use Flow\ETL\Runtime\Executor\WholeOutputHasher;
use Flow\ETL\Runtime\Metrics\DatasetHash;
use Flow\ETL\Runtime\Metrics\Hash;
use Flow\ETL\Runtime\Metrics\PartitionHash;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\Filesystem\Exception\InvalidArgumentException as PartitionException;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PartitionedOutputHasherTest extends TestCase
{
    public function test_each_partition_hashes_every_column_of_its_rows_over_batches(): void
    {
        $schema = schema(str_schema('country'), int_schema('id'));
        $pl = new WholeOutputHasher(HashAlgorithm::xxh128);
        $pl->add(array_to_rows([
            ['country' => 'PL', 'id' => 1],
            ['country' => 'PL', 'id' => 3],
        ], $schema), new PhpBackend());
        $de = new WholeOutputHasher(HashAlgorithm::xxh128);
        $de->add(array_to_rows([['country' => 'DE', 'id' => 2]], $schema), new PhpBackend());
        $hasher = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $hasher->add(array_to_rows([
            ['country' => 'PL', 'id' => 1],
            ['country' => 'DE', 'id' => 2],
        ], $schema), new PhpBackend());
        $hasher->add(array_to_rows([['country' => 'PL', 'id' => 3]], $schema), new PhpBackend());

        static::assertEquals(
            [
                new PartitionHash(new Partitions(new Partition('country', 'PL')), $pl->hash()->total),
                new PartitionHash(new Partitions(new Partition('country', 'DE')), $de->hash()->total),
            ],
            $hasher->hash()->partitions,
        );
    }

    public function test_the_total_equals_the_hash_of_the_whole_output(): void
    {
        $schema = schema(str_schema('country'), int_schema('id'));
        $rows = array_to_rows([
            ['country' => 'PL', 'id' => 1],
            ['country' => 'DE', 'id' => 2],
            ['country' => 'PL', 'id' => 3],
        ], $schema);
        $whole = new WholeOutputHasher(HashAlgorithm::xxh128);
        $whole->add($rows, new PhpBackend());
        $hasher = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $hasher->add($rows, new PhpBackend());

        static::assertEquals($whole->hash()->total, $hasher->hash()->total);
    }

    public function test_a_null_partition_value_is_one_partition_with_the_rows_of_the_null_folder(): void
    {
        $schema = schema(str_schema('country', nullable: true), int_schema('id'));
        $hasher = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $hasher->add(array_to_rows([
            ['country' => null, 'id' => 1],
            ['country' => null, 'id' => 2],
        ], $schema), new PhpBackend());

        static::assertEquals(
            [new Partitions(new Partition('country', null))],
            array_map(static fn(PartitionHash $hash): Partitions => $hash->partitions, $hasher->hash()->partitions),
        );
        static::assertSame(2, $hasher->hash()->partitions[0]->hash->rows);
    }

    public function test_null_and_the_null_folder_name_are_one_partition(): void
    {
        $schema = schema(str_schema('country', nullable: true), int_schema('id'));
        $whole = new WholeOutputHasher(HashAlgorithm::xxh128);
        $rows = array_to_rows([
            ['country' => null, 'id' => 1],
            ['country' => Partition::NULL_VALUE, 'id' => 2],
        ], $schema);
        $whole->add($rows, new PhpBackend());
        $hasher = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $hasher->add($rows, new PhpBackend());

        static::assertCount(1, $hasher->hash()->partitions);
        static::assertSame(2, $hasher->hash()->partitions[0]->hash->rows);
        static::assertEquals($whole->hash()->total, $hasher->hash()->partitions[0]->hash);
    }

    public function test_an_empty_string_and_null_are_two_partitions(): void
    {
        $hasher = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $hasher->add(
            array_to_rows(
                [
                    ['country' => '', 'id' => 1],
                    ['country' => null, 'id' => 2],
                ],
                schema(str_schema('country', nullable: true), int_schema('id')),
            ),
            new PhpBackend(),
        );

        static::assertSame(
            ['', null],
            array_map(
                static fn(PartitionHash $hash): ?string => $hash->partitions->toArray()[0]->value,
                $hasher->hash()->partitions,
            ),
        );
    }

    public function test_values_that_print_one_joined_partition_id_stay_apart(): void
    {
        $schema = schema(str_schema('a'), str_schema('b'));
        $hasher = new PartitionedOutputHasher(refs('a', 'b'), HashAlgorithm::xxh128);
        $hasher->add(array_to_rows([
            ['a' => 'x/b|y', 'b' => 'z'],
            ['a' => 'x', 'b' => 'y/b|z'],
            ['a' => 'x|b|y', 'b' => 'z'],
            ['a' => 'x', 'b' => 'y|b|z'],
        ], $schema), new PhpBackend());

        static::assertCount(4, $hasher->hash()->partitions);
    }

    public function test_one_datetime_instant_gives_one_partition_in_either_batch_order(): void
    {
        $schema = schema(datetime_schema('at'), int_schema('id'));
        $utc = array_to_rows([['at' => new DateTimeImmutable('2026-11-20T10:00:00+00:00'), 'id' => 1]], $schema);
        $warsaw = array_to_rows([['at' => new DateTimeImmutable('2026-11-20T12:00:00+02:00'), 'id' => 2]], $schema);
        $forward = new PartitionedOutputHasher(refs('at'), HashAlgorithm::xxh128);
        $forward->add($utc, new PhpBackend());
        $forward->add($warsaw, new PhpBackend());
        $backward = new PartitionedOutputHasher(refs('at'), HashAlgorithm::xxh128);
        $backward->add($warsaw, new PhpBackend());
        $backward->add($utc, new PhpBackend());

        static::assertCount(1, $forward->hash()->partitions);
        static::assertEquals($forward->hash(), $backward->hash());
    }

    public function test_a_later_batch_of_another_type_names_its_partition_in_the_declared_type(): void
    {
        $hasher = new PartitionedOutputHasher(refs('day'), HashAlgorithm::xxh128);
        $hasher->add(
            array_to_rows(
                [['day' => new DateTimeImmutable('2026-11-19'), 'id' => 1]],
                schema(date_schema('day'), int_schema('id')),
            ),
            new PhpBackend(),
        );
        $hasher->add(
            array_to_rows(
                [['day' => new DateTimeImmutable('2026-11-20 10:00:00'), 'id' => 2]],
                schema(datetime_schema('day'), int_schema('id')),
            ),
            new PhpBackend(),
        );

        static::assertSame(
            ['2026-11-19', '2026-11-20'],
            array_map(
                static fn(PartitionHash $hash): ?string => $hash->partitions->toArray()[0]->value,
                $hasher->hash()->partitions,
            ),
        );
    }

    public function test_a_float_partition_column_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'analyze()->withDatasetHash() cannot group by the float column "price": 0.0 and -0.0 are one value but two '
            . 'partition names. Cast it to a decimal string or an integer first.',
        );

        (new PartitionedOutputHasher(refs('price'), HashAlgorithm::xxh128))->add(array_to_rows([[
            'price' => 1.5,
        ]], schema(float_schema('price'))), new PhpBackend());
    }

    public function test_a_json_partition_column_is_refused(): void
    {
        $this->expectException(PartitionException::class);
        $this->expectExceptionMessage('Column "payload" of type json can\'t be used as a partition');

        (new PartitionedOutputHasher(refs('payload'), HashAlgorithm::xxh128))->add(array_to_rows([[
            'payload' => '{"a":1}',
        ]], schema(json_schema('payload'))), new PhpBackend());
    }

    public function test_a_first_batch_without_a_named_column_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'analyze()->withDatasetHash() groups by "country", but a batch has no column "country". Name only columns '
            . 'that every row of the output has.',
        );

        (new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128))->add(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id'))), new PhpBackend());
    }

    public function test_a_later_batch_without_a_nullable_named_column_hashes_into_the_null_partition(): void
    {
        $hasher = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $hasher->add(
            array_to_rows(
                [['country' => null, 'id' => 1]],
                schema(str_schema('country', nullable: true), int_schema('id')),
            ),
            new PhpBackend(),
        );
        $hasher->add(array_to_rows([['id' => 2]], schema(int_schema('id'))), new PhpBackend());

        static::assertCount(1, $hasher->hash()->partitions);
        static::assertSame(2, $hasher->hash()->partitions[0]->hash->rows);
    }

    public function test_a_later_batch_without_a_non_nullable_named_column_is_refused(): void
    {
        $hasher = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $hasher->add(
            array_to_rows([['country' => 'PL', 'id' => 1]], schema(str_schema('country'), int_schema('id'))),
            new PhpBackend(),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'analyze()->withDatasetHash() groups by "country", but a batch has no column "country".',
        );

        $hasher->add(array_to_rows([['id' => 2]], schema(int_schema('id'))), new PhpBackend());
    }

    public function test_an_empty_batch_first_does_not_fix_the_columns(): void
    {
        $hasher = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $hasher->add(array_to_rows([], schema(int_schema('id'))), new PhpBackend());
        $hasher->add(
            array_to_rows([['country' => 'PL', 'id' => 1]], schema(str_schema('country'), int_schema('id'))),
            new PhpBackend(),
        );

        static::assertCount(1, $hasher->hash()->partitions);
    }

    public function test_absorb_merges_a_partial_into_the_partition_with_the_same_values(): void
    {
        $schema = schema(str_schema('country'), int_schema('id'));
        $all = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $all->add(array_to_rows([
            ['country' => 'PL', 'id' => 1],
            ['country' => 'PL', 'id' => 2],
        ], $schema), new PhpBackend());
        $worker = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $worker->add(array_to_rows([['country' => 'PL', 'id' => 2]], $schema), new PhpBackend());
        $hasher = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $hasher->add(array_to_rows([['country' => 'PL', 'id' => 1]], $schema), new PhpBackend());
        $hasher->absorb($worker->hash());

        static::assertEquals($all->hash(), $hasher->hash());
    }

    public function test_absorb_into_an_empty_hasher_gives_the_partial_with_its_total(): void
    {
        $schema = schema(str_schema('country'), int_schema('id'));
        $worker = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $worker->add(array_to_rows([['country' => 'PL', 'id' => 2]], $schema), new PhpBackend());
        $hasher = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $hasher->absorb($worker->hash());

        static::assertEquals($worker->hash(), $hasher->hash());
    }

    public function test_describe_writes_no_state(): void
    {
        $hasher = new PartitionedOutputHasher(refs('country'), HashAlgorithm::xxh128);
        $hasher->add(
            array_to_rows([['country' => 'PL', 'id' => 1]], schema(str_schema('country'), int_schema('id'))),
            new PhpBackend(),
        );

        static::assertEquals(
            new DatasetHash(new Hash(0, '00000000000000000000000000000000'), [], HashAlgorithm::xxh128),
            DescribeRoundTrip::rebuilt($hasher)->hash(),
        );
    }

    public function test_a_rebuilt_hasher_keeps_its_algorithm(): void
    {
        $rebuilt = DescribeRoundTrip::rebuilt(new PartitionedOutputHasher(refs('country'), HashAlgorithm::md5));
        $rebuilt->add(
            array_to_rows([['country' => 'DE', 'id' => 2]], schema(str_schema('country'), int_schema('id'))),
            new PhpBackend(),
        );

        static::assertEquals(
            new DatasetHash(
                new Hash(1, '5e61d7a340ce02e52ce8b6900a2c5b18'),
                [
                    new PartitionHash(
                        new Partitions(new Partition('country', 'DE')),
                        new Hash(1, '5e61d7a340ce02e52ce8b6900a2c5b18'),
                    ),
                ],
                HashAlgorithm::md5,
            ),
            $rebuilt->hash(),
        );
    }

    public function test_an_aliased_partition_reference_fails_as_the_partitioned_loader_fails(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for column "day" not found.');

        (new PartitionedOutputHasher(refs(ref('date')->as('day')), HashAlgorithm::xxh128))->add(
            array_to_rows([['date' => '2026-11-20', 'id' => 1]], schema(str_schema('date'), int_schema('id'))),
            new PhpBackend(),
        );
    }
}
