<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\FloeWriter;
use Flow\Floe\Format;
use Flow\Floe\Options;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use Flow\Floe\Tests\Double\CodecStub;
use Flow\Floe\Tests\Double\UnsizedFilesystem;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function ord;
use function str_repeat;

final class FloeWriterTest extends TestCase
{
    public function test_create_for_stream_is_byte_identical_to_create_by_path(): void
    {
        $filesystem = memory_filesystem();
        $viaPath = path('memory://via-path.floe');
        $viaStream = path('memory://via-stream.floe');
        $data = rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'a']),
            row(['id' => 2, 'name' => 'b']),
        );
        $schema = $data->schema();

        $byPath = new FloeWriter($filesystem, $schema);
        $byPath->create($viaPath);
        $byPath->write($data);
        $byPath->close();

        $byStream = new FloeWriter($filesystem, $schema);
        $byStream->createForStream($filesystem->writeTo($viaStream));
        $byStream->write($data);
        $byStream->close();

        static::assertSame($filesystem->readFrom($viaPath)->content(), $filesystem->readFrom($viaStream)->content());
    }

    public function test_buffer_size_only_changes_flush_cadence_not_bytes(): void
    {
        $filesystem = memory_filesystem();
        $default = path('memory://default-buffer.floe');
        $tiny = path('memory://tiny-buffer.floe');
        $data = rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'alpha']),
            row(['id' => 2, 'name' => 'beta']),
        );

        $schema = $data->schema();

        $defaultWriter = new FloeWriter($filesystem, $schema);
        $defaultWriter->create($default);
        $defaultWriter->write($data);
        $defaultWriter->close();

        $tinyWriter = new FloeWriter($filesystem, $schema, new Options(bufferSize: 4));
        $tinyWriter->create($tiny);
        $tinyWriter->write($data);
        $tinyWriter->close();

        static::assertSame($filesystem->readFrom($default)->content(), $filesystem->readFrom($tiny)->content());
    }

    public function test_append_on_missing_file_behaves_like_create(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://missing.floe');

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->append($path);
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));
        $writer->close();

        static::assertSame(1, FloeStreamReaderContext::footer($filesystem, $path)->totalRows);
    }

    public function test_append_on_empty_file_behaves_like_create(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://zero-bytes.floe');
        $filesystem->writeTo($path)->close();

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->append($path);
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));
        $writer->close();

        static::assertSame(1, FloeStreamReaderContext::footer($filesystem, $path)->totalRows);
    }

    public function test_append_to_a_closed_zero_row_file_derives_the_schema_from_the_first_batch(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://zero-rows.floe');

        $writer = new FloeWriter($filesystem, schema());
        $writer->create($path);
        $writer->close();

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->append($path);
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));
        $writer->close();

        static::assertSame([['id' => 1]], FloeStreamReaderContext::readAll($filesystem, $path)->toArray());
    }

    public function test_append_to_a_zero_row_file_with_an_explicit_schema_validates_against_it(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://zero-rows-schema.floe');

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->create($path);
        $writer->close();

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->append($path);

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('new column "name"');

        $writer->write(rows(schema(str_schema('name')), row(['name' => 'flow'])));
    }

    public function test_create_with_an_explicit_schema_and_no_writes_records_the_schema_in_the_footer(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://schema-only.floe');

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->create($path);
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(0, $footer->totalRows);
        static::assertTrue($footer->schema()->isSame(schema(int_schema('id'))));
    }

    public function test_append_over_unsized_stream_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://unsized.floe');

        $writer = new FloeWriter($filesystem, schema());
        $writer->create($path);
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('requires a sized stream');

        (new FloeWriter(new UnsizedFilesystem($filesystem), schema()))->append($path);
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

        (new FloeWriter($filesystem, schema()))->append($path);
    }

    public function test_append_metadata_merges_over_existing_footer_metadata(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://meta.floe');

        $writer = new FloeWriter($filesystem, schema());
        $writer->create($path, Metadata::fromArray(['source' => 'create', 'kept' => 'yes']));
        $writer->close();
        $writer = new FloeWriter($filesystem, schema());
        $writer->append($path, Metadata::fromArray(['source' => 'append']));
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

        (new FloeWriter($filesystem, schema()))->append($path);
    }

    public function test_append_to_torn_file_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://torn.floe');
        $stream = $filesystem->writeTo($path);
        $stream->append(Format::header(0x00) . Format::frame(Format::FRAME_ROW, 'not closed'));
        $stream->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('torn');

        (new FloeWriter($filesystem, schema()))->append($path);
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

        (new FloeWriter($filesystem, schema()))->append($path);
    }

    public function test_append_with_incompatible_schema_keeps_previous_rows_readable(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://safe.floe');

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->create($path);
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));
        $writer->close();

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->append($path);

        try {
            $writer->write(rows(schema(str_schema('id')), row(['id' => 'no longer an int'])));
            static::fail('expected ' . IncompatibleSchemaException::class);
        } catch (IncompatibleSchemaException) {
        }

        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(1, $footer->totalRows);
        static::assertCount(1, $footer->sections);
    }

    public function test_append_with_new_non_nullable_column_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://evolve.floe');

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->create($path);
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));
        $writer->close();

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('new column "email"');

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->append($path);
        $writer->write(rows(schema(int_schema('id'), str_schema('email')), row(['id' => 2, 'email' => 'x'])));
    }

    public function test_append_with_new_nullable_column_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://merge.floe');

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->create($path);
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));
        $writer->close();

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->append($path);

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('new column "email"');

        $writer->write(rows(
            schema(int_schema('id'), str_schema('email', nullable: true)),
            row(['id' => 2, 'email' => null]),
        ));
    }

    public function test_append_with_same_schema_adds_a_section_reusing_the_footer_schema(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://same-schema.floe');

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->create($path);
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));
        $writer->close();

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->append($path);
        $writer->write(rows(schema(int_schema('id')), row(['id' => 2])));
        $writer->close();

        static::assertSame(
            [Format::FRAME_ROW, Format::FRAME_FOOTER, Format::FRAME_ROW, Format::FRAME_FOOTER],
            FloeStreamReaderContext::frameTypes($filesystem, $path),
        );

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(2, $footer->totalRows);
        static::assertNotSame([], $footer->schema);
        static::assertCount(2, $footer->sections);
    }

    public function test_close_twice_throws(): void
    {
        $writer = new FloeWriter(memory_filesystem(), schema());
        $writer->create(path('memory://closed.floe'));
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is not open');

        $writer->close();
    }

    public function test_opening_a_second_session_throws(): void
    {
        $writer = new FloeWriter(memory_filesystem(), schema());
        $writer->create(path('memory://first.floe'));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is already open');

        $writer->create(path('memory://second.floe'));
    }

    public function test_closing_empty_file_writes_header_and_footer_only(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://empty.floe');

        $writer = new FloeWriter($filesystem, schema());
        $writer->create($path);
        $writer->close();

        static::assertSame([Format::FRAME_FOOTER], FloeStreamReaderContext::frameTypes($filesystem, $path));

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(0, $footer->totalRows);
        static::assertSame([], $footer->sections);
        static::assertSame([], $footer->schema);
    }

    public function test_create_with_non_noop_codec_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('supports only the no-op codec, got codec 0x05');

        (new FloeWriter(memory_filesystem(), schema(), new Options(codec: new CodecStub(0x05))))->create(path(
            'memory://codec.floe',
        ));
    }

    public function test_two_writes_with_the_same_schema_in_one_session_share_one_section(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://continuation.floe');

        $writer = new FloeWriter($filesystem, schema(int_schema('id')));
        $writer->create($path);
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));
        $writer->write(rows(schema(int_schema('id')), row(['id' => 2])));
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(2, $footer->totalRows);
        static::assertCount(1, $footer->sections);
        static::assertSame(
            [Format::FRAME_ROW, Format::FRAME_ROW, Format::FRAME_FOOTER],
            FloeStreamReaderContext::frameTypes($filesystem, $path),
        );
    }

    public function test_nothing_reaches_the_stream_before_buffer_fills_or_close(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://buffered.floe');

        $writer = new FloeWriter($filesystem, schema(str_schema('data')));
        $writer->create($path);
        $writer->write(rows(schema(str_schema('data')), row(['data' => 'x'])));

        static::assertSame(0, $filesystem->readFrom($path)->size());

        $writer->write(rows(schema(str_schema('data')), row(['data' => str_repeat('x', 70_000)])));

        static::assertGreaterThan(0, $filesystem->readFrom($path)->size());
    }

    public function test_within_batch_heterogeneous_rows_encode_under_one_union_schema(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://sections.floe');

        $data = rows(
            schema(int_schema('a'), str_schema('b', nullable: true), str_schema('c', nullable: true)),
            row(['a' => 1, 'b' => 'x']),
            row(['a' => 2, 'c' => 'y']),
            row(['a' => 3, 'b' => 'z']),
        );

        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertNotSame([], $footer->schema);
        static::assertCount(1, $footer->sections);
        static::assertSame(3, $footer->totalRows);
        static::assertSame(3, $footer->sections[0]->rowCount);

        $fileSchema = $footer->schema();
        static::assertNotNull($fileSchema->findDefinition('a'));
        static::assertTrue($fileSchema->findDefinition('b')?->isNullable());
        static::assertTrue($fileSchema->findDefinition('c')?->isNullable());
        static::assertCount(3, FloeStreamReaderContext::readAll($filesystem, $path)->all());
    }

    public function test_section_offsets_point_at_frames(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offsets.floe');

        $data = rows(
            schema(int_schema('a', nullable: true), str_schema('b', nullable: true)),
            row(['a' => 1]),
            row(['b' => 'x']),
        );

        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);
        $source = $filesystem->readFrom($path);

        static::assertSame(Format::HEADER_LENGTH, $footer->sections[0]->offset);

        foreach ($footer->sections as $section) {
            static::assertSame(Format::FRAME_ROW, ord($source->read(1, $section->offset)));
        }
    }

    public function test_write_after_close_throws(): void
    {
        $writer = new FloeWriter(memory_filesystem(), schema());
        $writer->create(path('memory://done.floe'));
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is not open');

        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));
    }
}
