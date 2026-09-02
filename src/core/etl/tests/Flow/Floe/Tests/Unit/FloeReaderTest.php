<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\FloeMerger;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\Format;
use Flow\Floe\Options;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\Tests\Context\FloeSchemaContext;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use Flow\Floe\Tests\Double\CodecStub;
use Flow\Floe\Tests\Double\UnsizedFilesystem;
use Flow\Floe\Tests\Mother\FooterMother;
use Flow\Floe\Tests\Mother\RowsMother;
use PHPUnit\Framework\TestCase;

use function array_map;
use function array_slice;
use function count;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function pack;
use function strlen;

final class FloeReaderTest extends TestCase
{
    public function test_all_entry_types_round_trip(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://all-types.floe');
        $rows = RowsMother::withAllEntryTypes();

        $writer = new FloeWriter($filesystem, $rows->schema());
        $writer->create($path);
        $writer->write($rows);
        $writer->close();

        $batches = iterator_to_array(
            (new FloeReader($filesystem))
                ->read($path)
                ->rows(),
        );

        static::assertCount(1, $batches);
        static::assertEquals($rows->all(), $batches[0]->all());
    }

    public function test_batches_follow_requested_size(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://batches.floe');

        $data = rows(
            schema(int_schema('id')),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4]),
            row(['id' => 5]),
        );
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $reader = (new FloeReader($filesystem))->read($path);

        $sizes =
            /**
             * @param int<1, max> $batchSize
             *
             * @return array<int, int>
             */
            static fn(int $batchSize): array => array_map(
                static fn(Rows $batch): int => $batch->count(),
                iterator_to_array($reader->rows($batchSize)),
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

        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]), row(['id' => 4]));
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $ids = [];

        foreach ((new FloeReader($filesystem))
            ->read($path)
            ->rows(batchSize: 100, offset: 0, limit: 2) as $batch) {
            foreach ($batch->all() as $row) {
                $ids[] = $row->get('id');
            }
        }

        static::assertSame([1, 2], $ids);
    }

    public function test_limit_stops_within_a_batch(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://limit-batch.floe');

        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $batches = iterator_to_array(
            (new FloeReader($filesystem))
                ->read($path)
                ->rows(batchSize: 100, limit: 2),
        );

        static::assertCount(1, $batches);
        static::assertCount(2, $batches[0]->all());
    }

    public function test_negative_offset_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://neg-offset.floe');
        $writer = new FloeWriter($filesystem, schema());
        $writer->create($path);
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('offset must be greater or equal to 0');

        iterator_to_array(
            (new FloeReader($filesystem))
                ->read($path)
                ->rows(offset: -1),
        );
    }

    public function test_non_positive_limit_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://zero-limit.floe');
        $writer = new FloeWriter($filesystem, schema());
        $writer->create($path);
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('limit must be greater than 0');

        iterator_to_array(
            (new FloeReader($filesystem))
                ->read($path)
                ->rows(limit: 0),
        );
    }

    public function test_offset_skips_leading_rows_within_a_single_section(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-single.floe');

        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]), row(['id' => 4]));
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $ids = [];

        foreach ((new FloeReader($filesystem))
            ->read($path)
            ->rows(offset: 2) as $batch) {
            foreach ($batch->all() as $row) {
                $ids[] = $row->get('id');
            }
        }

        static::assertSame([3, 4], $ids);
    }

    public function test_offset_skips_whole_sections(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-sections.floe');

        $data = rows(
            schema(int_schema('id'), str_schema('email', nullable: true)),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4, 'email' => 'a']),
            row(['id' => 5, 'email' => 'b']),
            row(['id' => 6]),
            row(['id' => 7]),
        );
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $ids = [];
        $names = [];
        $emails = [];

        foreach ((new FloeReader($filesystem))
            ->read($path)
            ->rows(offset: 4) as $batch) {
            foreach ($batch->all() as $row) {
                $ids[] = $row->get('id');
                $names[] = $row->names();
                $emails[] = $row->get('email');
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

        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        static::assertSame(
            [],
            iterator_to_array(
                (new FloeReader($filesystem))
                    ->read($path)
                    ->rows(offset: 2),
            ),
        );
        static::assertSame(
            [],
            iterator_to_array(
                (new FloeReader($filesystem))
                    ->read($path)
                    ->rows(offset: 5),
            ),
        );
    }

    public function test_offset_respects_batch_size(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-batches.floe');

        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]), row(['id' => 4]));
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $sizes = array_map(
            static fn(Rows $batch): int => $batch->count(),
            iterator_to_array(
                (new FloeReader($filesystem))
                    ->read($path)
                    ->rows(batchSize: 1, offset: 1),
            ),
        );

        static::assertSame([1, 1, 1], $sizes);
    }

    public function test_offset_and_limit_combined(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-limit.floe');

        $data = rows(
            schema(int_schema('id')),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4]),
            row(['id' => 5]),
        );
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $ids = [];

        foreach ((new FloeReader($filesystem))
            ->read($path)
            ->rows(batchSize: 100, offset: 1, limit: 2) as $batch) {
            foreach ($batch->all() as $row) {
                $ids[] = $row->get('id');
            }
        }

        static::assertSame([2, 3], $ids);
    }

    public function test_offset_matches_tail_of_full_read(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-tail.floe');

        $data = rows(
            schema(int_schema('id'), str_schema('email', nullable: true)),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4, 'email' => 'a']),
            row(['id' => 5, 'email' => 'b']),
            row(['id' => 6]),
            row(['id' => 7]),
        );
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $reader = (new FloeReader($filesystem))->read($path);
        $full = [];

        foreach ($reader->rows() as $batch) {
            foreach ($batch->all() as $row) {
                $full[] = $row->get('id');
            }
        }

        for ($offset = 0; $offset <= 7; $offset++) {
            $tail = [];

            foreach ($reader->rows(offset: $offset) as $batch) {
                foreach ($batch->all() as $row) {
                    $tail[] = $row->get('id');
                }
            }

            static::assertSame(array_slice($full, $offset), $tail, "offset {$offset}");
        }
    }

    public function test_offset_reads_through_a_custom_identity_codec(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset-codec.floe');

        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));
        $writer = new FloeWriter($filesystem, $data->schema(), new Options(codec: new CodecStub(0x00)));
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $ids = [];

        foreach ((new FloeReader($filesystem, new CodecStub(0x00)))
            ->read($path)
            ->rows(offset: 1) as $batch) {
            foreach ($batch->all() as $extractedRow) {
                $ids[] = $extractedRow->get('id');
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
            Format::header(0x00) . Format::frame(Format::FRAME_ROW, "\xEE")
                . Format::frame(Format::FRAME_FOOTER, $footerJson . Format::trailer(strlen($footerJson))),
        );
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('unknown value flag');

        iterator_to_array(
            (new FloeReader($filesystem))
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
            (new FloeReader($filesystem))
                ->read($path)
                ->rows(),
        );
    }

    public function test_dead_footers_of_appended_files_are_skipped(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://appended.floe');

        $created = rows(schema(int_schema('id')), row(['id' => 1]));
        $writer = new FloeWriter($filesystem, $created->schema());
        $writer->create($path);
        $writer->write($created);
        $writer->close();

        $appended = rows(schema(int_schema('id')), row(['id' => 2]));
        $writer = new FloeWriter($filesystem, $appended->schema());
        $writer->append($path);
        $writer->write($appended);
        $writer->close();

        $batches = iterator_to_array(
            (new FloeReader($filesystem))
                ->read($path)
                ->rows(),
        );

        static::assertCount(1, $batches);
        static::assertSame([1, 2], array_map(static fn($row) => $row->get('id'), $batches[0]->all()));
    }

    public function test_empty_file_yields_no_batches(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://empty.floe');

        $writer = new FloeWriter($filesystem, schema());
        $writer->create($path);
        $writer->close();

        $reader = (new FloeReader($filesystem))->read($path);

        static::assertSame([], iterator_to_array($reader->rows()));
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

        $baseRows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
        $writer = new FloeWriter($filesystem, $baseRows->schema());
        $writer->create($baseFile);
        $writer->write($baseRows);
        $writer->close();

        $evolvedRows = rows(
            schema(int_schema('id'), str_schema('email', nullable: true)),
            row(['id' => 3, 'email' => null]),
            row(['id' => 4, 'email' => 'x@flow.php']),
        );
        $writer = new FloeWriter($filesystem, $evolvedRows->schema());
        $writer->create($evolvedFile);
        $writer->write($evolvedRows);
        $writer->close();

        (new FloeMerger($filesystem))->merge([$baseFile, $evolvedFile], $path);

        $batches = iterator_to_array(
            (new FloeReader($filesystem))
                ->read($path)
                ->rows(),
        );
        $rows = $batches[0]->all();

        static::assertCount(4, $rows);

        foreach ($rows as $row) {
            static::assertSame(['id', 'email'], $row->names());
        }

        static::assertNull($rows[0]->get('email'));
        static::assertSame('x@flow.php', $rows[3]->get('email'));

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

        (new FloeReader($filesystem))
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

        (new FloeReader($filesystem))
            ->read($path)
            ->footer();
    }

    public function test_footer_over_unsized_stream_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://unsized.floe');

        $writer = new FloeWriter($filesystem, schema());
        $writer->create($path);
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('requires a sized stream');

        (new FloeReader(new UnsizedFilesystem($filesystem)))
            ->read($path)
            ->footer();
    }

    public function test_footer_accessors(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://accessors.floe');

        $data = rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'x']));
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path, Metadata::fromArray(['source' => 'unit']));
        $writer->write($data);
        $writer->close();

        $reader = (new FloeReader($filesystem))->read($path);

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
        $writer = new FloeWriter($filesystem, schema());
        $writer->create($path);
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('supports only the no-op codec, got codec 0x09');

        (new FloeReader($filesystem, new CodecStub(0x09)))->read($path);
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

        (new FloeReader($filesystem))
            ->read($path)
            ->footer();
    }

    public function test_rows_through_a_custom_identity_codec(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://custom-codec.floe');

        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
        $writer = new FloeWriter($filesystem, $data->schema(), new Options(codec: new CodecStub(0x00)));
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $batches = iterator_to_array(
            (new FloeReader($filesystem, new CodecStub(0x00)))
                ->read($path)
                ->rows(),
        );

        static::assertCount(1, $batches);
        static::assertCount(2, $batches[0]->all());
    }

    public function test_rows_through_a_custom_identity_codec_with_long_row_body_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://custom-codec-long-row.floe');
        $schemaBody = FloeSchemaContext::schemaBody(schema(int_schema('id')));
        $rowBody = (new PhpFloeEncoder(schema_from_json(
            $schemaBody,
        )))->encode((new AdaptiveRowHydrator())->dehydrate(rows(schema(int_schema('id')), row(['id' => 1]))))[0];
        $footerJson = FooterMother::footer(schema: schema(int_schema('id'))->normalize())->toJson();
        $stream = $filesystem->writeTo($path);
        $stream->append(
            Format::header(0x00) . Format::frame(Format::FRAME_ROW, $rowBody . 'extra-bytes')
                . Format::frame(Format::FRAME_FOOTER, $footerJson . Format::trailer(strlen($footerJson))),
        );
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('row frame length does not match its content');

        iterator_to_array(
            (new FloeReader($filesystem, new CodecStub(0x00)))
                ->read($path)
                ->rows(),
        );
    }

    public function test_row_frame_not_described_by_the_footer_schema_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://row-first.floe');
        $footerJson = FooterMother::footer()->toJson();
        $stream = $filesystem->writeTo($path);
        $stream->append(
            Format::header(0x00) . Format::frame(Format::FRAME_ROW, 'row-bytes')
                . Format::frame(Format::FRAME_FOOTER, $footerJson . Format::trailer(strlen($footerJson))),
        );
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('row frame length does not match its content');

        iterator_to_array(
            (new FloeReader($filesystem))
                ->read($path)
                ->rows(),
        );
    }

    public function test_row_body_longer_than_hydrated_content_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://long-row.floe');
        $schemaBody = FloeSchemaContext::schemaBody(schema(int_schema('id')));
        $rowBody = (new PhpFloeEncoder(schema_from_json(
            $schemaBody,
        )))->encode((new AdaptiveRowHydrator())->dehydrate(rows(schema(int_schema('id')), row(['id' => 1]))))[0];
        $footerJson = FooterMother::footer(schema: schema(int_schema('id'))->normalize())->toJson();
        $stream = $filesystem->writeTo($path);
        $stream->append(
            Format::header(0x00) . Format::frame(Format::FRAME_ROW, $rowBody . 'extra-bytes')
                . Format::frame(Format::FRAME_FOOTER, $footerJson . Format::trailer(strlen($footerJson))),
        );
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('row frame length does not match its content');

        iterator_to_array(
            (new FloeReader($filesystem))
                ->read($path)
                ->rows(),
        );
    }

    public function test_torn_file_without_footer_throws_in_strict_mode(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://strict-torn.floe');

        FloeStreamReaderContext::writeWithoutFooter(
            $filesystem,
            $path,
            rows(schema(int_schema('id')), row(['id' => 1])),
        );

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('magic');

        iterator_to_array(
            (new FloeReader($filesystem))
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
            (new FloeReader($filesystem))
                ->read($path)
                ->rows(),
        );
    }

    public function test_head_returns_first_rows(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://head.floe');

        $data = rows(
            schema(int_schema('id')),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4]),
            row(['id' => 5]),
        );
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $ids = [];

        foreach ((new FloeReader($filesystem))
            ->read($path)
            ->head(3) as $batch) {
            foreach ($batch->all() as $row) {
                $ids[] = $row->get('id');
            }
        }

        static::assertSame([1, 2, 3], $ids);
    }

    public function test_head_count_beyond_total_returns_all_rows(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://head-all.floe');

        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $ids = [];

        foreach ((new FloeReader($filesystem))
            ->read($path)
            ->head(5) as $batch) {
            foreach ($batch->all() as $row) {
                $ids[] = $row->get('id');
            }
        }

        static::assertSame([1, 2], $ids);
    }

    public function test_head_respects_batch_size(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://head-batches.floe');

        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]), row(['id' => 4]));
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $sizes = array_map(
            static fn(Rows $batch): int => count($batch->all()),
            iterator_to_array(
                (new FloeReader($filesystem))
                    ->read($path)
                    ->head(3, batchSize: 1),
            ),
        );

        static::assertSame([1, 1, 1], $sizes);
    }

    public function test_head_non_positive_count_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://head-zero.floe');
        $writer = new FloeWriter($filesystem, schema());
        $writer->create($path);
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('head count must be greater than 0');

        iterator_to_array(
            (new FloeReader($filesystem))
                ->read($path)
                ->head(0),
        );
    }

    public function test_tail_returns_last_rows_across_sections(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://tail-sections.floe');

        $data = rows(
            schema(int_schema('id'), str_schema('email', nullable: true)),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4, 'email' => 'a']),
            row(['id' => 5, 'email' => 'b']),
            row(['id' => 6]),
            row(['id' => 7]),
        );
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $ids = [];
        $names = [];

        foreach ((new FloeReader($filesystem))
            ->read($path)
            ->tail(3) as $batch) {
            foreach ($batch->all() as $row) {
                $ids[] = $row->get('id');
                $names[] = $row->names();
            }
        }

        static::assertSame([5, 6, 7], $ids);
        static::assertSame(['id', 'email'], $names[0]);
    }

    public function test_tail_count_beyond_total_returns_all_rows(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://tail-all.floe');

        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $ids = [];

        foreach ((new FloeReader($filesystem))
            ->read($path)
            ->tail(5) as $batch) {
            foreach ($batch->all() as $row) {
                $ids[] = $row->get('id');
            }
        }

        static::assertSame([1, 2], $ids);
    }

    public function test_tail_respects_batch_size(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://tail-batches.floe');

        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]), row(['id' => 4]));
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $sizes = array_map(
            static fn(Rows $batch): int => count($batch->all()),
            iterator_to_array(
                (new FloeReader($filesystem))
                    ->read($path)
                    ->tail(3, batchSize: 1),
            ),
        );

        static::assertSame([1, 1, 1], $sizes);
    }

    public function test_tail_matches_slice_of_full_read(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://tail-slice.floe');

        $data = rows(
            schema(int_schema('id')),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4]),
            row(['id' => 5]),
            row(['id' => 6]),
            row(['id' => 7]),
        );
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $reader = (new FloeReader($filesystem))->read($path);

        $full = [];

        foreach ($reader->rows() as $batch) {
            foreach ($batch->all() as $row) {
                $full[] = $row->get('id');
            }
        }

        for ($count = 1; $count <= 9; $count++) {
            $tail = [];

            foreach ($reader->tail($count) as $batch) {
                foreach ($batch->all() as $row) {
                    $tail[] = $row->get('id');
                }
            }

            static::assertSame(array_slice($full, -$count), $tail, "tail {$count}");
        }
    }

    public function test_tail_non_positive_count_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://tail-zero.floe');
        $writer = new FloeWriter($filesystem, schema());
        $writer->create($path);
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('tail count must be greater than 0');

        iterator_to_array(
            (new FloeReader($filesystem))
                ->read($path)
                ->tail(0),
        );
    }
}
