<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Rows;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Partition;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\FloeMerger;
use Flow\Floe\FloeReader;
use Flow\Floe\Format;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use Flow\Floe\Tests\Double\UnsizedFilesystem;
use PHPUnit\Framework\TestCase;

use function array_map;
use function chr;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function pack;
use function str_repeat;

final class FloeMergerTest extends TestCase
{
    public function test_compact_reads_identically_to_splice(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            rows(row(int_entry('id', 1)), row(int_entry('id', 2))),
        );
        FloeStreamReaderContext::write($fs, path('memory://b.floe'), rows(row(int_entry('id', 3))));

        (new FloeMerger($fs))->merge([path('memory://a.floe'), path('memory://b.floe')], path('memory://splice.floe'));
        (new FloeMerger($fs))->merge(
            [path('memory://a.floe'), path('memory://b.floe')],
            path('memory://compact.floe'),
            compact: true,
        );

        static::assertEquals(
            FloeStreamReaderContext::readAll($fs, path('memory://splice.floe')),
            FloeStreamReaderContext::readAll($fs, path('memory://compact.floe')),
        );
    }

    public function test_compact_coalesces_same_schema_sources_into_one_section(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write($fs, path('memory://a.floe'), rows(row(int_entry('id', 1))));
        FloeStreamReaderContext::write($fs, path('memory://b.floe'), rows(row(int_entry('id', 2))));

        (new FloeMerger($fs))->merge(
            [path('memory://a.floe'), path('memory://b.floe')],
            path('memory://compact.floe'),
            compact: true,
        );

        static::assertCount(1, (new FloeReader($fs))->read(path('memory://compact.floe'))->footer()->sections);
    }

    public function test_merge_of_same_columns_in_different_order_re_encodes_instead_of_splicing(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write($fs, path('memory://a.floe'), rows(row(int_entry('a', 1), int_entry('b', 100))));
        FloeStreamReaderContext::write($fs, path('memory://b.floe'), rows(row(int_entry('b', 200), int_entry('a', 2))));

        (new FloeMerger($fs))->merge([path('memory://a.floe'), path('memory://b.floe')], path('memory://out.floe'));

        static::assertSame(
            [
                ['a' => 1, 'b' => 100],
                ['a' => 2, 'b' => 200],
            ],
            FloeStreamReaderContext::readAll($fs, path('memory://out.floe'))->toArray(),
        );
        // the compact path coalesces sources into one section - splicing would keep one per source
        static::assertCount(1, (new FloeReader($fs))->read(path('memory://out.floe'))->footer()->sections);
    }

    public function test_merge_of_same_columns_in_different_order_with_mixed_types_round_trips(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            rows(row(int_entry('a', 1), str_entry('b', 'one'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            rows(row(str_entry('b', 'two'), int_entry('a', 2))),
        );

        (new FloeMerger($fs))->merge([path('memory://a.floe'), path('memory://b.floe')], path('memory://out.floe'));

        static::assertSame(
            [
                ['a' => 1, 'b' => 'one'],
                ['a' => 2, 'b' => 'two'],
            ],
            FloeStreamReaderContext::readAll($fs, path('memory://out.floe'))->toArray(),
        );
    }

    public function test_empty_source_contributes_no_rows(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write($fs, path('memory://a.floe'), rows(row(int_entry('id', 1))));
        FloeStreamReaderContext::write($fs, path('memory://empty.floe'), rows());
        FloeStreamReaderContext::write($fs, path('memory://b.floe'), rows(row(int_entry('id', 2))));

        (new FloeMerger($fs))->merge([
            path('memory://a.floe'),
            path('memory://empty.floe'),
            path('memory://b.floe'),
        ], path('memory://out.floe'));

        static::assertEquals(
            rows(row(int_entry('id', 1)), row(int_entry('id', 2))),
            FloeStreamReaderContext::readAll($fs, path('memory://out.floe')),
        );
    }

    public function test_empty_sources_list_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('at least one source');

        (new FloeMerger(memory_filesystem()))->merge([], path('memory://out.floe'));
    }

    public function test_merging_a_source_that_adds_a_non_nullable_column_throws(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write($fs, path('memory://a.floe'), rows(row(int_entry('id', 1))));
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            rows(row(int_entry('id', 2), str_entry('email', 'x@y'))),
        );

        // "email" is non-nullable in B but absent from A - padding A's rows with null would
        // violate its required contract, so the merge is rejected rather than silently widened.
        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('Floe merge cannot reconcile the schema of');

        (new FloeMerger($fs))->merge([path('memory://a.floe'), path('memory://b.floe')], path('memory://out.floe'));
    }

    public function test_merging_a_source_that_drops_a_non_nullable_column_throws(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            rows(row(int_entry('id', 1), str_entry('email', 'x@y'))),
        );
        FloeStreamReaderContext::write($fs, path('memory://b.floe'), rows(row(int_entry('id', 2))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('Floe merge cannot reconcile the schema of');

        (new FloeMerger($fs))->merge([path('memory://a.floe'), path('memory://b.floe')], path('memory://out.floe'));
    }

    public function test_merging_sources_with_a_nullable_column_pads_missing_rows(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            rows(row(int_entry('id', 1), str_entry('email', null))),
        );
        FloeStreamReaderContext::write($fs, path('memory://b.floe'), rows(row(int_entry('id', 2))));

        (new FloeMerger($fs))->merge([path('memory://a.floe'), path('memory://b.floe')], path('memory://out.floe'));

        // "email" is nullable in A, so B's rows are legitimately padded with null on read-back.
        FloeStreamReaderContext::write(
            $fs,
            path('memory://reference.floe'),
            rows(row(int_entry('id', 1), str_entry('email', null)), row(int_entry('id', 2))),
        );

        static::assertEquals(
            FloeStreamReaderContext::readAll($fs, path('memory://reference.floe')),
            FloeStreamReaderContext::readAll($fs, path('memory://out.floe')),
        );
    }

    public function test_metadata_is_merged_with_new_keys_winning(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write($fs, path('memory://a.floe'), rows(row(int_entry('id', 1))), [
            'a' => '1',
            'shared' => 'from-a',
        ]);
        FloeStreamReaderContext::write($fs, path('memory://b.floe'), rows(row(int_entry('id', 2))), [
            'b' => '2',
            'shared' => 'from-b',
        ]);

        (new FloeMerger($fs))->merge(
            [path('memory://a.floe'), path('memory://b.floe')],
            path('memory://out.floe'),
            metadata: Metadata::fromArray(['shared' => 'override']),
        );

        static::assertSame(
            ['a' => '1', 'shared' => 'override', 'b' => '2'],
            (new FloeReader($fs))
                ->read(path('memory://out.floe'))
                ->metadata()
                ->normalize(),
        );
    }

    public function test_missing_source_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('does not exist');

        (new FloeMerger(memory_filesystem()))->merge([path('memory://missing.floe')], path('memory://out.floe'));
    }

    public function test_offset_read_after_merge_seeks_across_sources(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            rows(row(int_entry('id', 1)), row(int_entry('id', 2))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            rows(row(int_entry('id', 3)), row(int_entry('id', 4))),
        );

        (new FloeMerger($fs))->merge([path('memory://a.floe'), path('memory://b.floe')], path('memory://out.floe'));

        $read = [];

        foreach ((new FloeReader($fs))
            ->read(path('memory://out.floe'))
            ->rows(batchSize: 100, offset: 3) as $batch) {
            foreach ($batch as $r) {
                $read[] = $r->valueOf('id');
            }
        }

        static::assertSame([4], $read);
    }

    public function test_differing_partition_combinations_are_preserved(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://pl.floe'),
            Rows::partitioned([row(int_entry('id', 1), str_entry('c', 'PL'))], [new Partition('c', 'PL')]),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://us.floe'),
            Rows::partitioned([row(int_entry('id', 2), str_entry('c', 'US'))], [new Partition('c', 'US')]),
        );

        (new FloeMerger($fs))->merge([path('memory://pl.floe'), path('memory://us.floe')], path('memory://out.floe'));

        $file = (new FloeReader($fs))->read(path('memory://out.floe'));

        static::assertSame([['c' => 'PL'], ['c' => 'US']], $file->footer()->partitions);
        static::assertSame(0, $file->footer()->sections[0]->partitionsId);
        static::assertSame(1, $file->footer()->sections[1]->partitionsId);

        $combos = [];

        foreach ($file->rows() as $batch) {
            $combos[] = array_map(static fn(Partition $p): string => $p->value, $batch->partitions()->toArray());
        }

        static::assertSame([['PL'], ['US']], $combos);
    }

    public function test_unpartitioned_source_after_partitioned_resets_the_reader(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://pl.floe'),
            Rows::partitioned([row(int_entry('id', 1), str_entry('c', 'PL'))], [new Partition('c', 'PL')]),
        );
        // same schema {id, c} as pl, but written unpartitioned - so the merge exercises the
        // partition reset (partitioned -> unpartitioned) without a schema incompatibility
        FloeStreamReaderContext::write(
            $fs,
            path('memory://plain.floe'),
            rows(row(int_entry('id', 2), str_entry('c', 'XX'))),
        );

        (new FloeMerger($fs))->merge([
            path('memory://pl.floe'),
            path('memory://plain.floe'),
        ], path('memory://splice.floe'));
        (new FloeMerger($fs))->merge(
            [path('memory://pl.floe'), path('memory://plain.floe')],
            path('memory://compact.floe'),
            compact: true,
        );

        $combos = [];

        foreach ((new FloeReader($fs))
            ->read(path('memory://splice.floe'))
            ->rows() as $batch) {
            $combos[] = array_map(static fn(Partition $p): string => $p->value, $batch->partitions()->toArray());
        }

        static::assertSame([['PL'], []], $combos);
        static::assertEquals(
            FloeStreamReaderContext::readAll($fs, path('memory://compact.floe')),
            FloeStreamReaderContext::readAll($fs, path('memory://splice.floe')),
        );
    }

    public function test_partitioned_sources_merge_and_preserve_partition(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            Rows::partitioned([row(int_entry('id', 1), str_entry('c', 'PL'))], [new Partition('c', 'PL')]),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            Rows::partitioned([row(int_entry('id', 2), str_entry('c', 'PL'))], [new Partition('c', 'PL')]),
        );

        (new FloeMerger($fs))->merge([path('memory://a.floe'), path('memory://b.floe')], path('memory://out.floe'));

        $file = (new FloeReader($fs))->read(path('memory://out.floe'));

        static::assertSame([['c' => 'PL']], $file->footer()->partitions);
        static::assertSame(2, $file->totalRows());
    }

    public function test_single_source_is_a_copy(): void
    {
        $fs = memory_filesystem();
        $value = rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
        FloeStreamReaderContext::write($fs, path('memory://a.floe'), $value);

        (new FloeMerger($fs))->merge([path('memory://a.floe')], path('memory://out.floe'));

        static::assertEquals($value, FloeStreamReaderContext::readAll($fs, path('memory://out.floe')));
    }

    public function test_splices_three_files(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write($fs, path('memory://a.floe'), rows(row(int_entry('id', 1))));
        FloeStreamReaderContext::write($fs, path('memory://b.floe'), rows(row(int_entry('id', 2))));
        FloeStreamReaderContext::write($fs, path('memory://c.floe'), rows(row(int_entry('id', 3))));

        (new FloeMerger($fs))->merge([
            path('memory://a.floe'),
            path('memory://b.floe'),
            path('memory://c.floe'),
        ], path('memory://out.floe'));

        static::assertEquals(
            rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3))),
            FloeStreamReaderContext::readAll($fs, path('memory://out.floe')),
        );
    }

    public function test_splices_two_files_with_the_same_schema(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            rows(row(int_entry('id', 1)), row(int_entry('id', 2))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            rows(row(int_entry('id', 3)), row(int_entry('id', 4))),
        );

        (new FloeMerger($fs))->merge([path('memory://a.floe'), path('memory://b.floe')], path('memory://out.floe'));

        static::assertEquals(
            rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3)), row(int_entry('id', 4))),
            FloeStreamReaderContext::readAll($fs, path('memory://out.floe')),
        );
    }

    public function test_type_conflict_throws(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write($fs, path('memory://a.floe'), rows(row(int_entry('id', 1))));
        FloeStreamReaderContext::write($fs, path('memory://b.floe'), rows(row(str_entry('id', 'x'))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('Floe merge cannot reconcile the schema of');

        (new FloeMerger($fs))->merge([path('memory://a.floe'), path('memory://b.floe')], path('memory://out.floe'));
    }

    public function test_unsized_source_throws(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write($fs, path('memory://a.floe'), rows(row(int_entry('id', 1))));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('does not report its size');

        (new FloeMerger(new UnsizedFilesystem($fs)))->merge([path('memory://a.floe')], path('memory://out.floe'));
    }

    public function test_source_smaller_than_header_and_trailer_throws(): void
    {
        $fs = memory_filesystem();
        $fs
            ->writeTo(path('memory://tiny.floe'))
            ->append('FLOE' . chr(Format::VERSION) . "\x00")
            ->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('too small');

        (new FloeMerger($fs))->merge([path('memory://tiny.floe')], path('memory://out.floe'));
    }

    public function test_non_noop_codec_source_throws(): void
    {
        $fs = memory_filesystem();
        $fs
            ->writeTo(path('memory://codec.floe'))
            ->append('FLOE' . chr(Format::VERSION) . "\x05" . str_repeat("\0", 10))
            ->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('written with codec 0x05');

        (new FloeMerger($fs))->merge([path('memory://codec.floe')], path('memory://out.floe'));
    }

    public function test_source_with_footer_larger_than_file_throws(): void
    {
        $fs = memory_filesystem();
        $fs
            ->writeTo(path('memory://torn.floe'))
            ->append('FLOE' . chr(Format::VERSION) . "\x00" . pack('V', 1_000_000) . 'FLOE')
            ->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('footer does not fit');

        (new FloeMerger($fs))->merge([path('memory://torn.floe')], path('memory://out.floe'));
    }
}
