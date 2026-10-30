<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Storage;

use DateTimeImmutable;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\Floe\Exception\IncompatibleSchemaException;
use Flow\ETL\Storage\Floe\FloeFiles;
use Flow\ETL\Tests\Context\RowsStorageContext;
use Flow\ETL\Tests\Double\AcceptingBackend;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;

use function array_column;
use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function iterator_to_array;
use function range;

final class FilesystemRowsStorageTest extends FlowIntegrationTestCase
{
    public function test_sections_of_a_bucket_appended_in_batches(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_sections');
        $this->fs()->rm($cacheDir);
        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $batch = array_to_rows(
            array_map(static fn(int $id): array => ['id' => $id], range(1, 1000)),
            schema(int_schema('id')),
        );

        foreach (range(1, 250) as $_) {
            $storage->append('run', $batch);
        }

        static::assertSame([0, 100_000, 200_000], $storage->sections('run'));
        static::assertSame([], $storage->sections('missing'));
        $this->fs()->rm($cacheDir);
    }

    public function test_slice_from_an_offset(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_rows_offset');
        $this->fs()->rm($cacheDir);
        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend(), batchSize: 3);

        foreach ([[1, 2, 3], [4, 5], [6, 7, 8, 9]] as $ids) {
            $storage->append('run', array_to_rows(array_map(static fn(int $id): array => [
                'id' => $id,
            ], $ids), schema(int_schema('id'))));
        }

        static::assertSame([5, 6, 7, 8, 9], array_column(RowsStorageContext::rows($storage->slice('run', 4)), 'id'));
        static::assertSame([5, 6], array_column(RowsStorageContext::rows($storage->slice('run', 4, 2)), 'id'));
        static::assertSame([], RowsStorageContext::rows($storage->slice('missing', 0)));
        $this->fs()->rm($cacheDir);
    }

    public function test_remove_of_a_missing_bucket_is_a_no_op(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_remove_missing');
        $this->fs()->rm($cacheDir);
        (new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend()))->remove(
            'never-written',
        );

        static::assertNull($this->fs()->status($cacheDir));
    }

    public function test_append_after_get_does_not_truncate_the_bucket(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_reopen');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        static::assertCount(1, RowsStorageContext::rows($storage->get('bucket')));

        $storage->append('bucket', array_to_rows([['id' => 2]], schema(int_schema('id'))));

        static::assertCount(2, RowsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_append_with_a_column_absent_from_the_bucket_schema_fails(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_new_column');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
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

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows([['amount' => 1]], schema(int_schema('amount'))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessageMatches('/expected: amount<integer>, given: amount<float>/');

        $storage->append('bucket', array_to_rows([['amount' => 1.5]], schema(float_schema('amount'))));
    }

    public function test_appends_accumulate_into_one_bucket_in_order(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend(), batchSize: 2);
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 3]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 4], ['id' => 5]], schema(int_schema('id'))));

        static::assertSame([1, 2, 3, 4, 5], array_column(RowsStorageContext::rows($storage->get('bucket')), 'id'));

        $this->fs()->rm($cacheDir);
    }

    public function test_custom_batch_size_round_trips_all_rows(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_batch_size');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend(), batchSize: 2);
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id'))));

        static::assertCount(3, RowsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_small_appends_read_back_coalesced_up_to_the_batch_size(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_coalesced');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend(), batchSize: 3);

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
            RowsStorageContext::rows((new FilesystemRowsStorage(
                $this->fs(),
                cacheDir: $cacheDir,
                backend: new PhpBackend(),
            ))->get('nope')),
        );
    }

    public function test_remove_over_a_memory_filesystem_deletes_the_bucket(): void
    {
        $storage = new FilesystemRowsStorage(memory_filesystem(), path('memory://buckets/'), new PhpBackend());
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $storage->close('bucket');
        $storage->remove('bucket');

        static::assertFalse($storage->has('bucket'));
        static::assertSame([], RowsStorageContext::rows($storage->get('bucket')));
    }

    public function test_remove_closes_an_open_append_session(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_remove');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $storage->remove('bucket');

        static::assertSame([], RowsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_remove_deletes_the_bucket(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_remove');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $storage->remove('bucket');

        static::assertSame([], RowsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_round_trips_rows_across_the_read_batch_boundary(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_batch');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());

        $input = [];

        for ($i = 0; $i < 1500; $i++) {
            $input[] = ['id' => $i];
        }

        $storage->append('bucket', array_to_rows($input, schema(int_schema('id'))));

        static::assertSame($input, RowsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_an_empty_append_creates_a_readable_empty_bucket(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_empty');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', rows(schema()));
        $storage->close('bucket');

        static::assertTrue($storage->has('bucket'));
        static::assertSame(0, $storage->rows('bucket')->exactly());
        static::assertEquals(schema(), $storage->schema('bucket'));
        static::assertSame([], RowsStorageContext::rows($storage->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_has_rows_and_schema_describe_the_appended_batches(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_has_rows_schema');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 3]], schema(int_schema('id'))));

        static::assertTrue($storage->has('bucket'));
        static::assertSame(3, $storage->rows('bucket')->exactly());
        static::assertEquals(schema(int_schema('id')), $storage->schema('bucket'));
        static::assertFalse($storage->has('missing'));
        static::assertSame(0, $storage->rows('missing')->exactly());
        static::assertNull($storage->schema('missing'));

        $this->fs()->rm($cacheDir);
    }

    public function test_a_bucket_whose_writer_never_closed_is_missing_to_another_store(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_unclosed');
        $this->fs()->rm($cacheDir);

        $writing = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $writing->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $reading = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());

        static::assertTrue($writing->has('bucket'));
        static::assertFalse($reading->has('bucket'));

        $writing->close('bucket');

        static::assertTrue($reading->has('bucket'));

        $this->fs()->rm($cacheDir);
    }

    public function test_round_trips_schema_changing_rows(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_heterogeneous');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());

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
            RowsStorageContext::rows($storage->get('bucket')),
        );

        $this->fs()->rm($cacheDir);
    }

    public function test_append_with_reordered_structure_keys_matches_and_keeps_every_row(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_struct_order');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
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

        $read = RowsStorageContext::rows($storage->get('bucket'));

        static::assertCount(2, $read);
        static::assertSame(['a' => 1, 'b' => 'x'], $read[0]['s']);
        static::assertSame(['a' => 2, 'b' => 'y'], $read[1]['s']);

        $this->fs()->rm($cacheDir);
    }

    public function test_append_with_reordered_columns_matches_and_keeps_every_row(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_col_order');
        $this->fs()->rm($cacheDir);

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('bucket', array_to_rows(
            [['id' => 1, 'name' => 'x']],
            schema(int_schema('id'), str_schema('name')),
        ));
        $storage->append('bucket', array_to_rows(
            [['name' => 'y', 'id' => 2]],
            schema(str_schema('name'), int_schema('id')),
        ));

        $read = RowsStorageContext::rows($storage->get('bucket'));

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

        $writer = (new FloeFiles($this->fs(), new AdaptiveBackend()))->create(
            $path,
            ($first = array_to_rows(
                [['id' => 1, 'name' => 'x']],
                schema(int_schema('id'), str_schema('name')),
            ))->schema(),
        );
        $writer->write($first);
        $writer->close();

        $this->expectException(IncompatibleSchemaException::class);

        (new FloeFiles($this->fs(), new AdaptiveBackend()))->append($path, schema(
            int_schema('id'),
            str_schema('city'),
        ));
    }

    public function test_datetime_values_read_back_as_admitted(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_datetime_zone');
        $this->fs()->rm($cacheDir);

        $rows = array_to_rows([[
            'at' => new DateTimeImmutable('2026-01-02 03:04:05+05:00'),
        ]], schema(datetime_schema('at', zone: 'Europe/Warsaw')));
        $admitted = type_datetime()->assert($rows->column('at')->value(0))->format('c');

        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $storage->append('b', $rows);

        static::assertSame('2026-01-01T23:04:05+01:00', $admitted);
        static::assertSame(
            $admitted,
            type_datetime()->assert(RowsStorageContext::rows($storage->get('b'))[0]['at'])->format('c'),
        );

        $this->fs()->rm($cacheDir);
    }

    public function test_buckets_decode_with_the_given_backend(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_backend');
        $this->fs()->rm($cacheDir);
        $backend = new SpyBackend();
        $storage = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: $backend);
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        RowsStorageContext::rows($storage->get('bucket'));

        static::assertGreaterThan(0, $backend->decodeFrames());

        $this->fs()->rm($cacheDir);
    }

    public function test_close_finishes_a_bucket_so_a_fresh_store_can_read_it(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_close');
        $this->fs()->rm($cacheDir);
        $writer = new FilesystemRowsStorage($this->fs(), cacheDir: $cacheDir, backend: new PhpBackend());
        $writer->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $writer->close('bucket');
        $writer->close('never-written');

        static::assertCount(
            2,
            RowsStorageContext::rows((new FilesystemRowsStorage(
                $this->fs(),
                cacheDir: $cacheDir,
                backend: new PhpBackend(),
            ))->get('bucket')),
        );

        $this->fs()->rm($cacheDir);
    }

    public function test_a_store_over_the_local_filesystem_describes_its_root_and_batch_size(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_describe');
        $description = (new FilesystemRowsStorage(
            $this->fs(),
            cacheDir: $cacheDir,
            backend: new PhpBackend(),
            batchSize: 7,
        ))->describe();

        static::assertNotNull($description);
        static::assertTrue($description->sensitive());
        static::assertSame($cacheDir->normalize(), $description->data['root']);
        static::assertSame(7, $description->data['batch_size']);
    }

    public function test_a_backend_that_does_not_describe_makes_the_storage_undescribable(): void
    {
        static::assertNull(
            (new FilesystemRowsStorage(
                $this->fs(),
                cacheDir: path(__DIR__ . '/var/buckets_describe'),
                backend: new AcceptingBackend(),
            ))->describe(),
        );
    }

    public function test_a_store_over_a_memory_filesystem_does_not_describe(): void
    {
        static::assertNull(
            (new FilesystemRowsStorage(
                memory_filesystem(),
                cacheDir: path('memory://cache'),
                backend: new PhpBackend(),
            ))->describe(),
        );
    }
}
