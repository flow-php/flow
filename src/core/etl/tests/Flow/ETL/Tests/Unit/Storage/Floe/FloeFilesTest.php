<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Storage\Floe;

use DateTimeImmutable;
use Flow\ETL\Storage\Floe\Exception\IncompatibleSchemaException;
use Flow\ETL\Storage\Floe\FloeFiles;
use Flow\ETL\Storage\Floe\FloeMerger;
use Flow\ETL\Storage\Floe\FloeStreamWriter;
use Flow\ETL\Storage\Floe\Format;
use Flow\ETL\Storage\Floe\Options;
use Flow\ETL\Tests\Context\FloeStreamReaderContext;
use Flow\ETL\Tests\Double\CodecStub;
use Flow\ETL\Tests\Double\UnsizedStreamFilesystem;
use Flow\ETL\Tests\Mother\FooterMother;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Frame\FloeException;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema\Metadata;
use PHPUnit\Framework\TestCase;

use function array_map;
use function array_merge;
use function array_slice;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function ord;
use function pack;
use function str_repeat;
use function strlen;

final class FloeFilesTest extends TestCase
{
    public function test_all_column_types_round_trip(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://all-types.floe');
        $rows = RowsMother::withAllColumnTypes();

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $rows->schema());
        $writer->write($rows);
        $writer->close();

        $batches = iterator_to_array(
            (new FloeFiles($filesystem, new AdaptiveBackend()))
                ->read($path)
                ->rows(),
        );

        static::assertCount(1, $batches);
        static::assertEquals($rows->toArray(), $batches[0]->toArray());
    }

    public function test_batches_follow_requested_size(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://batches.floe');

        $data = array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
            ['id' => 5],
        ], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $data->schema());
        $writer->write($data);
        $writer->close();

        $reader = (new FloeFiles($filesystem, new AdaptiveBackend()))->read($path);

        $sizes =
            /**
             * @param int<1, max> $batchSize
             *
             * @return array<int, int>
             */
            static fn(int $batchSize): array => array_map(
                static fn(Rows $batch): int => $batch->count(),
                iterator_to_array($reader->rows($batchSize), false),
            );

        static::assertSame([2, 2, 1], $sizes(2));
        static::assertSame([5], $sizes(5));
        static::assertSame([1, 1, 1, 1, 1], $sizes(1));
        static::assertSame([5], $sizes(100));
    }

    public function test_limit_caps_returned_rows(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://limit.floe');

        $data = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $data->schema());
        $writer->write($data);
        $writer->close();

        $ids = [];

        foreach ((new FloeFiles($filesystem, new AdaptiveBackend()))
            ->read($path)
            ->rows(batchSize: 100, offset: 0, limit: 2) as $batch) {
            foreach ($batch->toArray() as $row) {
                $ids[] = $row['id'];
            }
        }

        static::assertSame([1, 2], $ids);
    }

    public function test_limit_stops_within_a_batch(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://limit-batch.floe');

        $data = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $data->schema());
        $writer->write($data);
        $writer->close();

        $batches = iterator_to_array(
            (new FloeFiles($filesystem, new AdaptiveBackend()))
                ->read($path)
                ->rows(batchSize: 100, limit: 2),
        );

        static::assertCount(1, $batches);
        static::assertCount(2, $batches[0]);
    }

    public function test_negative_offset_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://neg-offset.floe');
        (new FloeFiles($filesystem, new AdaptiveBackend()))
            ->create($path, schema())
            ->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('offset must be greater or equal to 0');

        iterator_to_array(
            (new FloeFiles($filesystem, new AdaptiveBackend()))
                ->read($path)
                ->rows(offset: -1),
        );
    }

    public function test_non_positive_limit_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://zero-limit.floe');
        (new FloeFiles($filesystem, new AdaptiveBackend()))
            ->create($path, schema())
            ->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('limit must be greater than 0');

        iterator_to_array(
            (new FloeFiles($filesystem, new AdaptiveBackend()))
                ->read($path)
                ->rows(limit: 0),
        );
    }

    public function test_offset_skips_leading_rows_within_a_single_section(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-single.floe');

        $data = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $data->schema());
        $writer->write($data);
        $writer->close();

        $ids = [];

        foreach ((new FloeFiles($filesystem, new AdaptiveBackend()))
            ->read($path)
            ->rows(offset: 2) as $batch) {
            foreach ($batch->toArray() as $row) {
                $ids[] = $row['id'];
            }
        }

        static::assertSame([3, 4], $ids);
    }

    public function test_offset_skips_whole_sections(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-sections.floe');

        $data = array_to_rows(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4, 'email' => 'a'],
                ['id' => 5, 'email' => 'b'],
                ['id' => 6],
                ['id' => 7],
            ],
            schema(int_schema('id'), str_schema('email', nullable: true)),
        );
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $data->schema());
        $writer->write($data);
        $writer->close();

        $ids = [];
        $names = [];
        $emails = [];

        foreach ((new FloeFiles($filesystem, new AdaptiveBackend()))
            ->read($path)
            ->rows(offset: 4) as $batch) {
            foreach ($batch->toArray() as $row) {
                $ids[] = $row['id'];
                $names[] = array_keys($row);
                $emails[] = $row['email'];
            }
        }

        static::assertSame([5, 6, 7], $ids);
        static::assertSame(['id', 'email'], $names[0]);
        static::assertSame('b', $emails[0]);
        static::assertNull($emails[1]);
    }

    public function test_offset_beyond_total_rows_yields_nothing(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-past-end.floe');

        $data = array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $data->schema());
        $writer->write($data);
        $writer->close();

        static::assertSame(
            [],
            iterator_to_array(
                (new FloeFiles($filesystem, new AdaptiveBackend()))
                    ->read($path)
                    ->rows(offset: 2),
            ),
        );
        static::assertSame(
            [],
            iterator_to_array(
                (new FloeFiles($filesystem, new AdaptiveBackend()))
                    ->read($path)
                    ->rows(offset: 5),
            ),
        );
    }

    public function test_offset_respects_batch_size(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-batches.floe');

        $data = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $data->schema());
        $writer->write($data);
        $writer->close();

        static::assertSame(
            [1, 1, 1],
            array_map(
                static fn(Rows $batch): int => $batch->count(),
                iterator_to_array(
                    (new FloeFiles($filesystem, new AdaptiveBackend()))
                        ->read($path)
                        ->rows(batchSize: 1, offset: 1),
                ),
            ),
        );
    }

    public function test_offset_and_limit_combined(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-limit.floe');

        $data = array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
            ['id' => 5],
        ], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $data->schema());
        $writer->write($data);
        $writer->close();

        $ids = [];

        foreach ((new FloeFiles($filesystem, new AdaptiveBackend()))
            ->read($path)
            ->rows(batchSize: 100, offset: 1, limit: 2) as $batch) {
            foreach ($batch->toArray() as $row) {
                $ids[] = $row['id'];
            }
        }

        static::assertSame([2, 3], $ids);
    }

    public function test_offset_matches_tail_of_full_read(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-tail.floe');

        $data = array_to_rows(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4, 'email' => 'a'],
                ['id' => 5, 'email' => 'b'],
                ['id' => 6],
                ['id' => 7],
            ],
            schema(int_schema('id'), str_schema('email', nullable: true)),
        );
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $data->schema());
        $writer->write($data);
        $writer->close();

        $reader = (new FloeFiles($filesystem, new AdaptiveBackend()))->read($path);
        $full = [];

        foreach ($reader->rows() as $batch) {
            foreach ($batch->toArray() as $row) {
                $full[] = $row['id'];
            }
        }

        for ($offset = 0; $offset <= 7; $offset++) {
            $tail = [];

            foreach ($reader->rows(offset: $offset) as $batch) {
                foreach ($batch->toArray() as $row) {
                    $tail[] = $row['id'];
                }
            }

            static::assertSame(array_slice($full, $offset), $tail, "offset {$offset}");
        }
    }

    public function test_offset_reads_through_a_custom_identity_codec(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-codec.floe');

        $data = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend(), new Options(codec: new CodecStub(0x00))))->create(
            $path,
            $data->schema(),
        );
        $writer->write($data);
        $writer->close();

        $ids = [];

        foreach ((new FloeFiles($filesystem, new AdaptiveBackend(), new Options(codec: new CodecStub(0x00))))
            ->read($path)
            ->rows(offset: 1) as $batch) {
            foreach ($batch->toArray() as $extractedRow) {
                $ids[] = $extractedRow['id'];
            }
        }

        static::assertSame([2, 3], $ids);
    }

    public function test_corrupt_row_body_throws_wrapped_exception(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://corrupt-row.floe');
        $footerJson = FooterMother::footer(schema: schema(int_schema('id'))->normalize())->toJson();
        $stream = $filesystem->writeTo($path);
        $stream->append(
            Format::header(0x00) . Format::frame(Format::FRAME_BATCH, "\xEE")
                . Format::frame(Format::FRAME_FOOTER, $footerJson . Format::trailer(strlen($footerJson))),
        );
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe BATCH frame is truncated, its directory is incomplete');

        iterator_to_array(
            (new FloeFiles($filesystem, new AdaptiveBackend()))
                ->read($path)
                ->rows(),
        );
    }

    public function test_corrupted_frame_chain_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://corrupted.floe');
        $footerJson = FooterMother::footer()->toJson();
        $stream = $filesystem->writeTo($path);
        $stream->append(
            Format::header(0x00) . "\x02" . pack('V', 10_000) . 'short'
                . Format::frame(Format::FRAME_FOOTER, $footerJson . Format::trailer(strlen($footerJson))),
        );
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('frame body is incomplete');

        iterator_to_array(
            (new FloeFiles($filesystem, new AdaptiveBackend()))
                ->read($path)
                ->rows(),
        );
    }

    public function test_dead_footers_of_appended_files_are_skipped(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://appended.floe');

        $created = array_to_rows([['id' => 1]], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $created->schema());
        $writer->write($created);
        $writer->close();

        $appended = array_to_rows([['id' => 2]], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, $appended->schema());
        $writer->write($appended);
        $writer->close();

        static::assertSame(
            [[['id' => 1]], [['id' => 2]]],
            array_map(
                static fn(Rows $batch) => $batch->toArray(),
                iterator_to_array(
                    (new FloeFiles($filesystem, new AdaptiveBackend()))
                        ->read($path)
                        ->rows(),
                    false,
                ),
            ),
        );
    }

    public function test_empty_file_yields_no_batches(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://empty.floe');

        (new FloeFiles($filesystem, new AdaptiveBackend()))
            ->create($path, schema())
            ->close();

        $reader = (new FloeFiles($filesystem, new AdaptiveBackend()))->read($path);

        static::assertSame([], iterator_to_array($reader->rows(), false));
        static::assertSame(0, $reader->totalRows());
        static::assertCount(0, $reader->schema()->definitions());
    }

    public function test_evolved_file_rows_match_merged_schema(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://evolved.floe');

        // a multi-schema file is what FloeMerger produces (schema evolution lives in merge, not the writer)
        $baseFile = path('memory://evolved-base.floe');
        $evolvedFile = path('memory://evolved-new.floe');

        $baseRows = array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($baseFile, $baseRows->schema());
        $writer->write($baseRows);
        $writer->close();

        $evolvedRows = array_to_rows(
            [['id' => 3, 'email' => null], ['id' => 4, 'email' => 'x@flow.php']],
            schema(int_schema('id'), str_schema('email', nullable: true)),
        );
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($evolvedFile, $evolvedRows->schema());
        $writer->write($evolvedRows);
        $writer->close();

        (new FloeMerger($filesystem, new AdaptiveBackend()))->merge([$baseFile, $evolvedFile], $path);

        $batches = iterator_to_array(
            (new FloeFiles($filesystem, new AdaptiveBackend()))
                ->read($path)
                ->rows(),
            false,
        );

        static::assertSame(
            [
                ['id' => 1, 'email' => null],
                ['id' => 2, 'email' => null],
                ['id' => 3, 'email' => null],
                ['id' => 4, 'email' => 'x@flow.php'],
            ],
            array_merge(...array_map(static fn(Rows $batch) => $batch->toArray(), $batches)),
        );

        // the merged schema is what carries nullability now, not a per-cell definition
        $merged = $batches[0]->schema();
        static::assertTrue($merged->get('email')->isNullable());
        static::assertTrue($merged->get('email')->metadata()->isEmpty());
    }

    public function test_footer_length_exceeding_file_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://bad-footer-length.floe');
        $stream = $filesystem->writeTo($path);
        $stream->append(Format::header(0x00) . Format::trailer(9999));
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('footer does not fit inside the file');

        (new FloeFiles($filesystem, new AdaptiveBackend()))
            ->read($path)
            ->footer();
    }

    public function test_footer_of_file_smaller_than_header_and_trailer_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://stub.floe');
        $stream = $filesystem->writeTo($path);
        $stream->append('FLOE');
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('too small');

        (new FloeFiles($filesystem, new AdaptiveBackend()))
            ->read($path)
            ->footer();
    }

    public function test_footer_over_unsized_stream_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://unsized.floe');

        (new FloeFiles($filesystem, new AdaptiveBackend()))
            ->create($path, schema())
            ->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('requires a sized stream');

        (new FloeFiles(new UnsizedStreamFilesystem($filesystem), new AdaptiveBackend()))
            ->read($path)
            ->footer();
    }

    public function test_footer_accessors(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://accessors.floe');

        $data = array_to_rows([['id' => 1, 'name' => 'x']], schema(int_schema('id'), str_schema('name')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create(
            $path,
            $data->schema(),
            Metadata::fromArray(['source' => 'unit']),
        );
        $writer->write($data);
        $writer->close();

        $reader = (new FloeFiles($filesystem, new AdaptiveBackend()))->read($path);

        static::assertSame(Format::VERSION, $reader->footer()->version);
        static::assertSame(['source' => 'unit'], $reader->metadata()->normalize());
        static::assertSame(1, $reader->totalRows());
        static::assertNotNull($reader->schema()->findDefinition('id'));
        static::assertNotNull($reader->schema()->findDefinition('name'));
    }

    public function test_open_with_non_noop_codec_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://x.floe');
        (new FloeFiles($filesystem, new AdaptiveBackend()))
            ->create($path, schema())
            ->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('supports only the no-op codec, got codec 0x09');

        (new FloeFiles($filesystem, new AdaptiveBackend(), new Options(codec: new CodecStub(0x09))))->read($path);
    }

    public function test_reader_with_mismatched_codec_flags_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://flags.floe');
        $stream = $filesystem->writeTo($path);
        $stream->append(Format::header(0x07) . Format::trailer(0));
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('written with codec 0x07');

        (new FloeFiles($filesystem, new AdaptiveBackend()))
            ->read($path)
            ->footer();
    }

    public function test_rows_through_a_custom_identity_codec(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://custom-codec.floe');

        $data = array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend(), new Options(codec: new CodecStub(0x00))))->create(
            $path,
            $data->schema(),
        );
        $writer->write($data);
        $writer->close();

        $batches = iterator_to_array(
            (new FloeFiles($filesystem, new AdaptiveBackend(), new Options(codec: new CodecStub(0x00))))
                ->read($path)
                ->rows(),
        );

        static::assertCount(1, $batches);
        static::assertCount(2, $batches[0]);
    }

    public function test_torn_file_without_footer_throws_in_strict_mode(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://strict-torn.floe');

        FloeStreamReaderContext::writeWithoutFooter($filesystem, $path, array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id'))));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('magic');

        iterator_to_array(
            (new FloeFiles($filesystem, new AdaptiveBackend()))
                ->read($path)
                ->rows(),
        );
    }

    public function test_unknown_frame_type_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://unknown-frame.floe');
        $footerJson = FooterMother::footer()->toJson();
        $stream = $filesystem->writeTo($path);
        $stream->append(
            Format::header(0x00) . Format::frame(0x55, 'mystery')
                . Format::frame(Format::FRAME_FOOTER, $footerJson . Format::trailer(strlen($footerJson))),
        );
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('unknown frame type 0x55');

        iterator_to_array(
            (new FloeFiles($filesystem, new AdaptiveBackend()))
                ->read($path)
                ->rows(),
        );
    }

    public function test_create_for_stream_is_byte_identical_to_create_by_path(): void
    {
        $filesystem = memory_filesystem();
        $viaPath = path('memory://via-path.floe');
        $viaStream = path('memory://via-stream.floe');
        $data = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']],
            schema(int_schema('id'), str_schema('name')),
        );
        $schema = $data->schema();

        $byPath = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($viaPath, $schema);
        $byPath->write($data);
        $byPath->close();

        $byStream = FloeStreamWriter::create($filesystem->writeTo($viaStream), $schema, new AdaptiveBackend());
        $byStream->write($data);
        $byStream->close();

        static::assertSame($filesystem->readFrom($viaPath)->content(), $filesystem->readFrom($viaStream)->content());
    }

    public function test_buffer_size_only_changes_flush_cadence_not_bytes(): void
    {
        $filesystem = memory_filesystem();
        $default = path('memory://default-buffer.floe');
        $tiny = path('memory://tiny-buffer.floe');
        $data = array_to_rows(
            [['id' => 1, 'name' => 'alpha'], ['id' => 2, 'name' => 'beta']],
            schema(int_schema('id'), str_schema('name')),
        );

        $schema = $data->schema();

        $defaultWriter = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($default, $schema);
        $defaultWriter->write($data);
        $defaultWriter->close();

        $tinyWriter = (new FloeFiles($filesystem, new AdaptiveBackend(), new Options(bufferSize: 4)))->create(
            $tiny,
            $schema,
        );
        $tinyWriter->write($data);
        $tinyWriter->close();

        static::assertSame($filesystem->readFrom($default)->content(), $filesystem->readFrom($tiny)->content());
    }

    public function test_append_on_missing_file_behaves_like_create(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://missing.floe');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, schema(int_schema('id')));
        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $writer->close();

        static::assertSame(1, FloeStreamReaderContext::footer($filesystem, $path)->statistics->rows);
    }

    public function test_append_on_empty_file_behaves_like_create(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://zero-bytes.floe');
        $filesystem->writeTo($path)->close();

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, schema(int_schema('id')));
        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $writer->close();

        static::assertSame(1, FloeStreamReaderContext::footer($filesystem, $path)->statistics->rows);
    }

    public function test_append_to_a_closed_zero_row_file_derives_the_schema_from_the_first_batch(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://zero-rows.floe');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, schema());
        $writer->close();

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, schema(int_schema('id')));
        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $writer->close();

        static::assertSame([['id' => 1]], FloeStreamReaderContext::readAll($filesystem, $path)->toArray());
    }

    public function test_append_to_a_zero_row_file_with_an_explicit_schema_validates_against_it(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://zero-rows-schema.floe');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, schema(int_schema('id')));
        $writer->close();

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, schema(int_schema('id')));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('new column "name"');

        $writer->write(array_to_rows([['name' => 'flow']], schema(str_schema('name'))));
    }

    public function test_create_with_an_explicit_schema_and_no_writes_records_the_schema_in_the_footer(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://schema-only.floe');

        (new FloeFiles($filesystem, new AdaptiveBackend()))
            ->create($path, schema(int_schema('id')))
            ->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(0, $footer->statistics->rows);
        static::assertTrue($footer->schema()->isSame(schema(int_schema('id'))));
    }

    public function test_append_over_unsized_stream_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://unsized.floe');

        (new FloeFiles($filesystem, new AdaptiveBackend()))
            ->create($path, schema())
            ->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('requires a sized stream');

        (new FloeFiles(new UnsizedStreamFilesystem($filesystem), new AdaptiveBackend()))->append($path, schema());
    }

    public function test_append_with_footer_length_exceeding_file_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://bad-footer-length.floe');
        $stream = $filesystem->writeTo($path);
        $stream->append(Format::header(0x00) . Format::trailer(9999));
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('footer does not fit inside the file');

        (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, schema());
    }

    public function test_append_metadata_merges_over_existing_footer_metadata(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://meta.floe');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create(
            $path,
            schema(),
            Metadata::fromArray(['source' => 'create', 'kept' => 'yes']),
        );
        $writer->close();
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->append(
            $path,
            schema(),
            Metadata::fromArray(['source' => 'append']),
        );
        $writer->close();

        static::assertSame(
            ['source' => 'append', 'kept' => 'yes'],
            FloeStreamReaderContext::footer($filesystem, $path)->metadata->normalize(),
        );
    }

    public function test_append_to_file_with_different_codec_flags_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://flags.floe');
        $stream = $filesystem->writeTo($path);
        $stream->append(Format::header(0x05) . Format::trailer(0));
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('written with codec 0x05');

        (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, schema());
    }

    public function test_append_to_torn_file_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://torn.floe');
        $stream = $filesystem->writeTo($path);
        $stream->append(Format::header(0x00) . Format::frame(Format::FRAME_BATCH, 'not closed'));
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('torn');

        (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, schema());
    }

    public function test_append_to_truncated_stub_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://stub.floe');
        $stream = $filesystem->writeTo($path);
        $stream->append('FLOE');
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('too small');

        (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, schema());
    }

    public function test_append_with_incompatible_schema_keeps_previous_rows_readable(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://safe.floe');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, schema(int_schema('id')));
        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $writer->close();

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, schema(int_schema('id')));

        try {
            $writer->write(array_to_rows([['id' => 'no longer an int']], schema(str_schema('id'))));
            static::fail('expected ' . IncompatibleSchemaException::class);
        } catch (IncompatibleSchemaException) {
        }

        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(1, $footer->statistics->rows);
        static::assertCount(1, $footer->sections);
    }

    public function test_append_with_new_non_nullable_column_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://evolve.floe');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, schema(int_schema('id')));
        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $writer->close();

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('new column "email"');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, schema(int_schema('id')));
        $writer->write(array_to_rows([['id' => 2, 'email' => 'x']], schema(int_schema('id'), str_schema('email'))));
    }

    public function test_append_with_new_nullable_column_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://merge.floe');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, schema(int_schema('id')));
        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $writer->close();

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, schema(int_schema('id')));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('new column "email"');

        $writer->write(array_to_rows(
            [['id' => 2, 'email' => null]],
            schema(int_schema('id'), str_schema('email', nullable: true)),
        ));
    }

    public function test_append_with_same_schema_adds_a_section_reusing_the_footer_schema(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://same-schema.floe');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, schema(int_schema('id')));
        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $writer->close();

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->append($path, schema(int_schema('id')));
        $writer->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
        $writer->close();

        static::assertSame(
            [Format::FRAME_BATCH, Format::FRAME_FOOTER, Format::FRAME_BATCH, Format::FRAME_FOOTER],
            FloeStreamReaderContext::frameTypes($filesystem, $path),
        );

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(2, $footer->statistics->rows);
        static::assertNotSame([], $footer->schema);
        static::assertCount(2, $footer->sections);
    }

    public function test_close_twice_throws(): void
    {
        $writer = (new FloeFiles(memory_filesystem(), new AdaptiveBackend()))->create(
            path('memory://closed.floe'),
            schema(),
        );
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is closed');

        $writer->close();
    }

    public function test_closing_empty_file_writes_header_and_footer_only(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://empty.floe');

        (new FloeFiles($filesystem, new AdaptiveBackend()))
            ->create($path, schema())
            ->close();

        static::assertSame([Format::FRAME_FOOTER], FloeStreamReaderContext::frameTypes($filesystem, $path));

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(0, $footer->statistics->rows);
        static::assertSame([], $footer->sections);
        static::assertSame([], $footer->schema);
    }

    public function test_create_with_non_noop_codec_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('supports only the no-op codec, got codec 0x05');

        (new FloeFiles(memory_filesystem(), new AdaptiveBackend(), new Options(codec: new CodecStub(0x05))))->create(
            path('memory://codec.floe'),
            schema(),
        );
    }

    public function test_two_writes_with_the_same_schema_in_one_session_share_one_section(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://continuation.floe');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, schema(int_schema('id')));
        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $writer->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(2, $footer->statistics->rows);
        static::assertCount(1, $footer->sections);
        static::assertSame(
            [Format::FRAME_BATCH, Format::FRAME_BATCH, Format::FRAME_FOOTER],
            FloeStreamReaderContext::frameTypes($filesystem, $path),
        );
    }

    public function test_nothing_reaches_the_stream_before_buffer_fills_or_close(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://buffered.floe');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, schema(str_schema('data')));
        $writer->write(array_to_rows([['data' => 'x']], schema(str_schema('data'))));

        static::assertSame(0, $filesystem->readFrom($path)->size());

        $writer->write(array_to_rows([['data' => str_repeat('x', 70_000)]], schema(str_schema('data'))));

        static::assertGreaterThan(0, $filesystem->readFrom($path)->size());
    }

    public function test_within_batch_heterogeneous_rows_encode_under_one_union_schema(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://sections.floe');

        $data = array_to_rows(
            [['a' => 1, 'b' => 'x'], ['a' => 2, 'c' => 'y'], ['a' => 3, 'b' => 'z']],
            schema(int_schema('a'), str_schema('b', nullable: true), str_schema('c', nullable: true)),
        );

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $data->schema());
        $writer->write($data);
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertNotSame([], $footer->schema);
        static::assertCount(1, $footer->sections);
        static::assertSame(3, $footer->statistics->rows);
        static::assertSame(3, $footer->sections[0]->rowCount);

        $fileSchema = $footer->schema();
        static::assertNotNull($fileSchema->findDefinition('a'));
        static::assertTrue($fileSchema->findDefinition('b')?->isNullable());
        static::assertTrue($fileSchema->findDefinition('c')?->isNullable());
        static::assertCount(3, FloeStreamReaderContext::readAll($filesystem, $path));
    }

    public function test_section_offsets_point_at_frames(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offsets.floe');

        $data = array_to_rows(
            [['a' => 1], ['b' => 'x']],
            schema(int_schema('a', nullable: true), str_schema('b', nullable: true)),
        );

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $data->schema());
        $writer->write($data);
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);
        $source = $filesystem->readFrom($path);

        static::assertSame(Format::HEADER_LENGTH, $footer->sections[0]->offset);

        foreach ($footer->sections as $section) {
            static::assertSame(Format::FRAME_BATCH, ord($source->read(1, $section->offset)));
        }
    }

    public function test_write_after_close_throws(): void
    {
        $writer = (new FloeFiles(memory_filesystem(), new AdaptiveBackend()))->create(
            path('memory://done.floe'),
            schema(),
        );
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is closed');

        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
    }

    public function test_append_refuses_a_different_datetime_zone(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://zoned.floe');

        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, schema(datetime_schema('at')));
        $writer->write(array_to_rows([[
            'at' => new DateTimeImmutable('2026-01-02 03:04:05'),
        ]], schema(datetime_schema('at'))));
        $writer->close();

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('Floe append schema does not match the existing file schema.');

        (new FloeFiles($filesystem, new AdaptiveBackend()))->append(
            $path,
            schema(datetime_schema('at', zone: 'Europe/Warsaw')),
        );
    }
}
