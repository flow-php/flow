<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Bucketing\Storage;

use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Row;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\FloeWriter;

use function array_map;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\structure_entry;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class FilesystemBucketsTest extends FlowIntegrationTestCase
{
    public function test_append_after_get_does_not_truncate_the_bucket(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_reopen');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir);
        $storage->append('bucket', rows(row(int_entry('id', 1))));

        static::assertCount(1, BucketsStorageContext::rows($storage->get('bucket')));

        $storage->append('bucket', rows(row(int_entry('id', 2))));

        static::assertCount(2, BucketsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_append_with_a_column_absent_from_the_bucket_schema_fails(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_new_column');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir);
        $storage->append('bucket', rows(row(int_entry('id', 1))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessageMatches('/new column "name"/');

        $storage->append('bucket', rows(row(int_entry('id', 2), str_entry('name', 'John'))));
    }

    public function test_append_widening_a_column_type_fails(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_widen');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir);
        $storage->append('bucket', rows(row(int_entry('amount', 1))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessageMatches(
            '/column "amount" \(row 0\): could not convert 1\.5 \(float\) to integer/',
        );

        $storage->append('bucket', rows(row(float_entry('amount', 1.5))));
    }

    public function test_appends_accumulate_into_one_bucket_in_order(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, batchSize: 2);
        $storage->append('bucket', rows(row(int_entry('id', 1)), row(int_entry('id', 2))));
        $storage->append('bucket', rows(row(int_entry('id', 3))));
        $storage->append('bucket', rows(row(int_entry('id', 4)), row(int_entry('id', 5))));

        static::assertSame(
            [1, 2, 3, 4, 5],
            array_map(static fn(Row $r): mixed => $r->valueOf(
                'id',
            ), BucketsStorageContext::rows($storage->get('bucket'))),
        );

        $this->fs()->rm($cacheDir);
    }

    public function test_custom_batch_size_round_trips_all_rows(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_batch_size');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir, batchSize: 2);
        $storage->append('bucket', rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3))));

        static::assertCount(3, BucketsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_get_missing_bucket_yields_nothing(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_missing');
        $this->fs()->rm($cacheDir);

        static::assertSame(
            [],
            BucketsStorageContext::rows((new FilesystemBuckets($this->fs(), cacheDir: $cacheDir))->get('nope')),
        );
    }

    public function test_remove_closes_an_open_append_session(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_remove');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir);
        $storage->append('bucket', rows(row(int_entry('id', 1))));
        $storage->remove('bucket');

        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_remove_deletes_the_bucket(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_remove');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir);
        $storage->append('bucket', rows(row(int_entry('id', 1))));
        $storage->remove('bucket');

        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_round_trips_rows_across_the_read_batch_boundary(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_batch');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir);

        $input = [];

        for ($i = 0; $i < 1500; $i++) {
            $input[] = row(int_entry('id', $i));
        }

        $storage->append('bucket', rows(...$input));

        static::assertEquals($input, BucketsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_set_replaces_a_bucket_written_with_append(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_set');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir);
        $storage->append('bucket', rows(row(int_entry('id', 1)), row(int_entry('id', 2))));
        $storage->set('bucket', rows(row(int_entry('id', 3))));

        static::assertSame(
            [3],
            array_map(static fn(Row $r): mixed => $r->valueOf(
                'id',
            ), BucketsStorageContext::rows($storage->get('bucket'))),
        );

        $this->fs()->rm($cacheDir);
    }

    public function test_set_with_empty_rows_creates_a_readable_empty_bucket(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_empty');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir);
        $storage->set('bucket', rows());

        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_round_trips_schema_changing_rows(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_heterogeneous');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir);

        $input = [
            row(int_entry('id', 1)),
            row(int_entry('id', 2), str_entry('name', 'John')),
            row(str_entry('name', 'Jane')),
        ];

        $storage->append('bucket', rows(...$input));

        // one write session = one schema: rows keep their own columns (unpadded) but entry
        // types widen to the batch union, so compare values rather than exact definitions
        static::assertSame(
            array_map(static fn(Row $r): array => $r->toArray(), $input),
            array_map(static fn(Row $r): array => $r->toArray(), BucketsStorageContext::rows($storage->get('bucket'))),
        );

        $this->fs()->rm($cacheDir);
    }

    public function test_append_with_reordered_structure_keys_conforms_and_keeps_every_row(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_struct_order');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir);
        $storage->append(
            'bucket',
            rows(row(int_entry('id', 1), structure_entry('s', ['a' => 1, 'b' => 'x'], type_structure([
                'a' => type_integer(),
                'b' => type_string(),
            ])))),
        );
        $storage->append(
            'bucket',
            rows(row(int_entry('id', 2), structure_entry('s', ['b' => 'y', 'a' => 2], type_structure([
                'b' => type_string(),
                'a' => type_integer(),
            ])))),
        );

        $read = BucketsStorageContext::rows($storage->get('bucket'));

        static::assertCount(2, $read);
        static::assertSame(['a' => 1, 'b' => 'x'], $read[0]->get('s')->value());
        static::assertSame(['a' => 2, 'b' => 'y'], $read[1]->get('s')->value());

        $this->fs()->rm($cacheDir);
    }

    public function test_append_with_reordered_columns_conforms_and_keeps_every_row(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_col_order');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemBuckets($this->fs(), cacheDir: $cacheDir);
        $storage->append('bucket', rows(row(int_entry('id', 1), str_entry('name', 'x'))));
        $storage->append('bucket', rows(row(str_entry('name', 'y'), int_entry('id', 2))));

        $read = BucketsStorageContext::rows($storage->get('bucket'));

        static::assertCount(2, $read);
        static::assertSame([['id' => 1, 'name' => 'x'], ['id' => 2, 'name' => 'y']], [
            $read[0]->toArray(),
            $read[1]->toArray(),
        ]);

        $this->fs()->rm($cacheDir);
    }

    public function test_append_resume_with_a_genuinely_different_column_set_still_throws(): void
    {
        $path = $this->cacheDir->suffix('resume-different-columns.floe');

        $writer = new FloeWriter(
            $this->fs(),
            ($first = rows(row(int_entry('id', 1), str_entry('name', 'x'))))->schema(),
        );
        $writer->create($path);
        $writer->write($first);
        $writer->close();

        $writer = new FloeWriter($this->fs(), rows(row(int_entry('id', 2), str_entry('city', 'y')))->schema());

        $this->expectException(IncompatibleSchemaException::class);

        $writer->append($path);
    }
}
