<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Bucketing\Storage;

use DateTimeImmutable;
use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\FloeWriter;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function iterator_to_array;

final class FilesystemBucketsTest extends FlowIntegrationTestCase
{
    public function test_append_after_get_does_not_truncate_the_bucket(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_reopen');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        static::assertCount(1, BucketsStorageContext::rows($storage->get('bucket')));

        $storage->append('bucket', array_to_rows([['id' => 2]], schema(int_schema('id'))));

        static::assertCount(2, BucketsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_append_with_a_column_absent_from_the_bucket_schema_fails(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_new_column');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessageMatches('/new column "name"/');

        $storage->append('bucket', array_to_rows(
            [['id' => 2, 'name' => 'John']],
            schema(int_schema('id'), str_schema('name')),
        ));
    }

    public function test_append_widening_a_column_type_fails(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_widen');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows([['amount' => 1]], schema(int_schema('amount'))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessageMatches('/expected: amount<integer>, given: amount<float>/');

        $storage->append('bucket', array_to_rows([['amount' => 1.5]], schema(float_schema('amount'))));
    }

    public function test_appends_accumulate_into_one_bucket_in_order(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend(), batchSize: 2);
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 3]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 4], ['id' => 5]], schema(int_schema('id'))));

        static::assertSame([1, 2, 3, 4, 5], array_column(BucketsStorageContext::rows($storage->get('bucket')), 'id'));

        $this->fs()->rm($cacheDir);
    }

    public function test_custom_batch_size_round_trips_all_rows(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_batch_size');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend(), batchSize: 2);
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id'))));

        static::assertCount(3, BucketsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_small_appends_read_back_coalesced_up_to_the_batch_size(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_coalesced');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend(), batchSize: 3);

        foreach ([1, 2, 3, 4, 5] as $id) {
            $storage->append('bucket', array_to_rows([['id' => $id]], schema(int_schema('id'))));
        }

        static::assertSame(
            [[1, 2, 3], [4, 5]],
            array_map(
                static fn(Rows $batch) => $batch->reduceToArray('id'),
                iterator_to_array($storage->get('bucket'), false),
            ),
        );

        $this->fs()->rm($cacheDir);
    }

    public function test_get_missing_bucket_yields_nothing(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_missing');
        $this->fs()->rm($cacheDir);

        static::assertSame(
            [],
            BucketsStorageContext::rows((new FilesystemBuckets(
                $this->fs(),
                cacheDir: $cacheDir,
                backend: new PhpBackend(),
            ))->get('nope')),
        );
    }

    public function test_remove_closes_an_open_append_session(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_remove');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $storage->remove('bucket');

        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_remove_deletes_the_bucket(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_remove');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $storage->remove('bucket');

        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_round_trips_rows_across_the_read_batch_boundary(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_batch');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());

        $input = [];

        for ($i = 0; $i < 1500; $i++) {
            $input[] = ['id' => $i];
        }

        $storage->append('bucket', array_to_rows($input, schema(int_schema('id'))));

        static::assertSame($input, BucketsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_set_replaces_a_bucket_written_with_append(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_set');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->set('bucket', array_to_rows([['id' => 3]], schema(int_schema('id'))));

        static::assertSame([3], array_column(BucketsStorageContext::rows($storage->get('bucket')), 'id'));

        $this->fs()->rm($cacheDir);
    }

    public function test_set_with_empty_rows_creates_a_readable_empty_bucket(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_empty');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->set('bucket', rows(schema()));

        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_round_trips_schema_changing_rows(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_heterogeneous');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());

        $storage->append('bucket', array_to_rows(
            [['id' => 1], ['id' => 2, 'name' => 'John'], ['name' => 'Jane']],
            schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
        ));

        // one write session = one schema, and every row now carries every column the schema declares -
        // a column a row omitted comes back as the null its nullable declaration allows
        static::assertSame(
            [
                ['id' => 1, 'name' => null],
                ['id' => 2, 'name' => 'John'],
                ['id' => null, 'name' => 'Jane'],
            ],
            BucketsStorageContext::rows($storage->get('bucket')),
        );

        $this->fs()->rm($cacheDir);
    }

    public function test_append_with_reordered_structure_keys_matches_and_keeps_every_row(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_struct_order');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows(
            [['id' => 1, 's' => ['a' => 1, 'b' => 'x']]],
            schema(
                int_schema('id'),
                structure_schema('s', type_structure([
                    'a' => type_integer(),
                    'b' => type_string(),
                ])),
            ),
        ));
        $storage->append('bucket', array_to_rows(
            [['id' => 2, 's' => ['b' => 'y', 'a' => 2]]],
            schema(
                int_schema('id'),
                structure_schema('s', type_structure([
                    'b' => type_string(),
                    'a' => type_integer(),
                ])),
            ),
        ));

        $read = BucketsStorageContext::rows($storage->get('bucket'));

        static::assertCount(2, $read);
        static::assertSame(['a' => 1, 'b' => 'x'], $read[0]['s']);
        static::assertSame(['a' => 2, 'b' => 'y'], $read[1]['s']);

        $this->fs()->rm($cacheDir);
    }

    public function test_append_with_reordered_columns_matches_and_keeps_every_row(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_col_order');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows(
            [['id' => 1, 'name' => 'x']],
            schema(int_schema('id'), str_schema('name')),
        ));
        $storage->append('bucket', array_to_rows(
            [['name' => 'y', 'id' => 2]],
            schema(str_schema('name'), int_schema('id')),
        ));

        $read = BucketsStorageContext::rows($storage->get('bucket'));

        static::assertCount(2, $read);
        static::assertSame([['id' => 1, 'name' => 'x'], ['id' => 2, 'name' => 'y']], [
            $read[0],
            $read[1],
        ]);

        $this->fs()->rm($cacheDir);
    }

    public function test_append_resume_with_a_genuinely_different_column_set_still_throws(): void
    {
        $path = $this->cacheDir->suffix('resume-different-columns.floe');

        $writer = new FloeWriter(
            $this->fs(),
            ($first = array_to_rows(
                [['id' => 1, 'name' => 'x']],
                schema(int_schema('id'), str_schema('name')),
            ))->schema(),
            new AdaptiveBackend(),
        );
        $writer->create($path);
        $writer->write($first);
        $writer->close();

        $writer = new FloeWriter(
            $this->fs(),
            array_to_rows([['id' => 2, 'city' => 'y']], schema(int_schema('id'), str_schema('city')))->schema(),
            new AdaptiveBackend(),
        );

        $this->expectException(IncompatibleSchemaException::class);

        $writer->append($path);
    }

    public function test_datetime_values_read_back_as_admitted(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_datetime_zone');
        $this->fs()->rm($cacheDir);

        $rows = array_to_rows([[
            'at' => new DateTimeImmutable('2026-01-02 03:04:05+05:00'),
        ]], schema(datetime_schema('at', zone: 'Europe/Warsaw')));
        $admitted = type_datetime()->assert($rows->column('at')->value(0))->format('c');

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->set('b', $rows);

        static::assertSame('2026-01-01T23:04:05+01:00', $admitted);
        static::assertSame(
            $admitted,
            type_datetime()->assert(BucketsStorageContext::rows($storage->get('b'))[0]['at'])->format('c'),
        );

        $this->fs()->rm($cacheDir);
    }

    public function test_buckets_decode_with_the_given_backend(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_backend');
        $this->fs()->rm($cacheDir);
        $backend = new SpyBackend();
        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, backend: $backend);
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        BucketsStorageContext::rows($storage->get('bucket'));

        static::assertGreaterThan(0, $backend->decodes());

        $this->fs()->rm($cacheDir);
    }
}
