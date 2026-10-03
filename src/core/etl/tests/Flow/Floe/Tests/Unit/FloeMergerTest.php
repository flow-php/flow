<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\FloeMerger;
use Flow\Floe\FloeReader;
use Flow\Floe\Format;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use Flow\Floe\Tests\Double\UnsizedFilesystem;
use PHPUnit\Framework\TestCase;

use function chr;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_datetime;
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
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['id' => 3]], schema(int_schema('id'))),
        );

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([
            path('memory://a.floe'),
            path('memory://b.floe'),
        ], path('memory://splice.floe'));
        (new FloeMerger($fs, new AdaptiveBackend()))->merge(
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
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['id' => 2]], schema(int_schema('id'))),
        );

        (new FloeMerger($fs, new AdaptiveBackend()))->merge(
            [path('memory://a.floe'), path('memory://b.floe')],
            path('memory://compact.floe'),
            compact: true,
        );

        static::assertCount(
            1,
            (new FloeReader($fs, new AdaptiveBackend()))->read(path('memory://compact.floe'))->footer()->sections,
        );
    }

    public function test_merge_of_same_columns_in_different_order_re_encodes_instead_of_splicing(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows([['a' => 1, 'b' => 100]], schema(int_schema('a'), int_schema('b'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['b' => 200, 'a' => 2]], schema(int_schema('b'), int_schema('a'))),
        );

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([
            path('memory://a.floe'),
            path('memory://b.floe'),
        ], path('memory://out.floe'));

        static::assertSame(
            [
                ['a' => 1, 'b' => 100],
                ['a' => 2, 'b' => 200],
            ],
            FloeStreamReaderContext::readAll($fs, path('memory://out.floe'))->toArray(),
        );
        // the compact path coalesces sources into one section - splicing would keep one per source
        static::assertCount(
            1,
            (new FloeReader($fs, new AdaptiveBackend()))->read(path('memory://out.floe'))->footer()->sections,
        );
    }

    public function test_merge_of_same_columns_in_different_order_with_mixed_types_round_trips(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows([['a' => 1, 'b' => 'one']], schema(int_schema('a'), str_schema('b'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['b' => 'two', 'a' => 2]], schema(str_schema('b'), int_schema('a'))),
        );

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([
            path('memory://a.floe'),
            path('memory://b.floe'),
        ], path('memory://out.floe'));

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
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
        );
        FloeStreamReaderContext::write($fs, path('memory://empty.floe'), rows(schema()));
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['id' => 2]], schema(int_schema('id'))),
        );

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([
            path('memory://a.floe'),
            path('memory://empty.floe'),
            path('memory://b.floe'),
        ], path('memory://out.floe'));

        static::assertEquals(
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
            FloeStreamReaderContext::readAll($fs, path('memory://out.floe')),
        );
    }

    public function test_empty_sources_list_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('at least one source');

        (new FloeMerger(memory_filesystem(), new AdaptiveBackend()))->merge([], path('memory://out.floe'));
    }

    public function test_merging_a_source_that_adds_a_non_nullable_column_throws(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['id' => 2, 'email' => 'x@y']], schema(int_schema('id'), str_schema('email'))),
        );

        // "email" is non-nullable in B but absent from A - padding A's rows with null would
        // violate its required contract, so the merge is rejected rather than silently widened.
        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('Floe merge cannot reconcile the schema of');

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([
            path('memory://a.floe'),
            path('memory://b.floe'),
        ], path('memory://out.floe'));
    }

    public function test_merging_a_source_that_drops_a_non_nullable_column_throws(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows([['id' => 1, 'email' => 'x@y']], schema(int_schema('id'), str_schema('email'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['id' => 2]], schema(int_schema('id'))),
        );

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('Floe merge cannot reconcile the schema of');

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([
            path('memory://a.floe'),
            path('memory://b.floe'),
        ], path('memory://out.floe'));
    }

    public function test_merging_sources_with_a_nullable_column_pads_missing_rows(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows(
                [['id' => 1, 'email' => null]],
                schema(int_schema('id'), str_schema('email', nullable: true)),
            ),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['id' => 2]], schema(int_schema('id'))),
        );

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([
            path('memory://a.floe'),
            path('memory://b.floe'),
        ], path('memory://out.floe'));

        // "email" is nullable in A, so B's rows are legitimately padded with null on read-back.
        FloeStreamReaderContext::write(
            $fs,
            path('memory://reference.floe'),
            array_to_rows(
                [['id' => 1, 'email' => null], ['id' => 2]],
                schema(int_schema('id'), str_schema('email', nullable: true)),
            ),
        );

        static::assertEquals(
            FloeStreamReaderContext::readAll($fs, path('memory://reference.floe')),
            FloeStreamReaderContext::readAll($fs, path('memory://out.floe')),
        );
    }

    public function test_metadata_is_merged_with_new_keys_winning(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
            [
                'a' => '1',
                'shared' => 'from-a',
            ],
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['id' => 2]], schema(int_schema('id'))),
            [
                'b' => '2',
                'shared' => 'from-b',
            ],
        );

        (new FloeMerger($fs, new AdaptiveBackend()))->merge(
            [path('memory://a.floe'), path('memory://b.floe')],
            path('memory://out.floe'),
            metadata: Metadata::fromArray(['shared' => 'override']),
        );

        static::assertSame(
            ['a' => '1', 'shared' => 'override', 'b' => '2'],
            (new FloeReader($fs, new AdaptiveBackend()))
                ->read(path('memory://out.floe'))
                ->metadata()
                ->normalize(),
        );
    }

    public function test_missing_source_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('does not exist');

        (new FloeMerger(memory_filesystem(), new AdaptiveBackend()))->merge([path(
            'memory://missing.floe',
        )], path('memory://out.floe'));
    }

    public function test_offset_read_after_merge_seeks_across_sources(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['id' => 3], ['id' => 4]], schema(int_schema('id'))),
        );

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([
            path('memory://a.floe'),
            path('memory://b.floe'),
        ], path('memory://out.floe'));

        $read = [];

        foreach ((new FloeReader($fs, new AdaptiveBackend()))
            ->read(path('memory://out.floe'))
            ->rows(batchSize: 100, offset: 3) as $batch) {
            $read = [...$read, ...$batch->column('id')->values()];
        }

        static::assertSame([4], $read);
    }

    public function test_single_source_is_a_copy(): void
    {
        $fs = memory_filesystem();
        $value = array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
        FloeStreamReaderContext::write($fs, path('memory://a.floe'), $value);

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([path('memory://a.floe')], path('memory://out.floe'));

        static::assertEquals($value, FloeStreamReaderContext::readAll($fs, path('memory://out.floe')));
    }

    public function test_splices_three_files(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['id' => 2]], schema(int_schema('id'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://c.floe'),
            array_to_rows([['id' => 3]], schema(int_schema('id'))),
        );

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([
            path('memory://a.floe'),
            path('memory://b.floe'),
            path('memory://c.floe'),
        ], path('memory://out.floe'));

        static::assertEquals(
            array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id'))),
            FloeStreamReaderContext::readAll($fs, path('memory://out.floe')),
        );
    }

    public function test_splices_two_files_with_the_same_schema(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['id' => 3], ['id' => 4]], schema(int_schema('id'))),
        );

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([
            path('memory://a.floe'),
            path('memory://b.floe'),
        ], path('memory://out.floe'));

        static::assertEquals(
            array_to_rows([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]], schema(int_schema('id'))),
            FloeStreamReaderContext::readAll($fs, path('memory://out.floe')),
        );
    }

    public function test_type_conflict_throws(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://b.floe'),
            array_to_rows([['id' => 'x']], schema(str_schema('id'))),
        );

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('Floe merge cannot reconcile the schema of');

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([
            path('memory://a.floe'),
            path('memory://b.floe'),
        ], path('memory://out.floe'));
    }

    public function test_unsized_source_throws(): void
    {
        $fs = memory_filesystem();
        FloeStreamReaderContext::write(
            $fs,
            path('memory://a.floe'),
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
        );

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('does not report its size');

        (new FloeMerger(new UnsizedFilesystem($fs), new AdaptiveBackend()))->merge([path(
            'memory://a.floe',
        )], path('memory://out.floe'));
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

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([path('memory://tiny.floe')], path('memory://out.floe'));
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

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([path('memory://codec.floe')], path('memory://out.floe'));
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

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([path('memory://torn.floe')], path('memory://out.floe'));
    }

    public function test_merge_of_different_datetime_zones_widens_to_utc(): void
    {
        $fs = memory_filesystem();
        $warsaw = new DateTimeImmutable('2026-01-02 03:04:05', new DateTimeZone('Europe/Warsaw'));
        $utc = new DateTimeImmutable('2026-01-02 03:04:05', new DateTimeZone('UTC'));
        FloeStreamReaderContext::write(
            $fs,
            path('memory://warsaw.floe'),
            array_to_rows([['at' => $warsaw]], schema(datetime_schema('at', zone: 'Europe/Warsaw'))),
        );
        FloeStreamReaderContext::write(
            $fs,
            path('memory://utc.floe'),
            array_to_rows([['at' => $utc]], schema(datetime_schema('at'))),
        );

        (new FloeMerger($fs, new AdaptiveBackend()))->merge([
            path('memory://warsaw.floe'),
            path('memory://utc.floe'),
        ], path('memory://merged.floe'));

        $merged = FloeStreamReaderContext::readAll($fs, path('memory://merged.floe'));

        static::assertSame(
            'datetime',
            FloeStreamReaderContext::footer($fs, path('memory://merged.floe'))->schema()->get('at')->type()->toString(),
        );
        static::assertSame([$warsaw->getTimestamp(), $utc->getTimestamp()], [
            type_datetime()->assert($merged->column('at')->value(0))->getTimestamp(),
            type_datetime()->assert($merged->column('at')->value(1))->getTimestamp(),
        ]);
    }
}
