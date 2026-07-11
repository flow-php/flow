<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Rows;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Partition;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\FloeWriter;
use Flow\Floe\Format;
use Flow\Floe\Tests\Context\FloeFileContext;
use Flow\Floe\Tests\Double\CodecStub;
use Flow\Floe\Tests\Double\UnsizedFilesystem;
use PHPUnit\Framework\TestCase;

use function array_map;
use function array_values;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function ord;
use function str_repeat;

final class FloeWriterTest extends TestCase
{
    public function test_append_on_missing_file_behaves_like_create(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://missing.floe');

        $writer = new FloeWriter($filesystem);
        $writer->append($path);
        $writer->write(rows(row(int_entry('id', 1))));
        $writer->close();

        static::assertSame(1, FloeFileContext::footer($filesystem, $path)->totalRows);
    }

    public function test_append_on_empty_file_behaves_like_create(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://zero-bytes.floe');
        $filesystem->writeTo($path)->close();

        $writer = new FloeWriter($filesystem);
        $writer->append($path);
        $writer->write(rows(row(int_entry('id', 1))));
        $writer->close();

        static::assertSame(1, FloeFileContext::footer($filesystem, $path)->totalRows);
    }

    public function test_append_over_unsized_stream_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://unsized.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path);
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('requires a sized stream');

        (new FloeWriter(new UnsizedFilesystem($filesystem)))->append($path);
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

        (new FloeWriter($filesystem))->append($path);
    }

    public function test_append_metadata_merges_over_existing_footer_metadata(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://meta.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path, Metadata::fromArray(['source' => 'create', 'kept' => 'yes']));
        $writer->close();
        $writer = new FloeWriter($filesystem);
        $writer->append($path, Metadata::fromArray(['source' => 'append']));
        $writer->close();

        static::assertSame(
            ['source' => 'append', 'kept' => 'yes'],
            FloeFileContext::footer($filesystem, $path)->metadata->normalize(),
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

        (new FloeWriter($filesystem))->append($path);
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

        (new FloeWriter($filesystem))->append($path);
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

        (new FloeWriter($filesystem))->append($path);
    }

    public function test_append_with_incompatible_schema_keeps_previous_rows_readable(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://safe.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path);
        $writer->write(rows(row(int_entry('id', 1))));
        $writer->close();

        $writer = new FloeWriter($filesystem);
        $writer->append($path);

        try {
            $writer->write(rows(row(str_entry('id', 'no longer an int'))));
            static::fail('expected ' . IncompatibleSchemaException::class);
        } catch (IncompatibleSchemaException) {
        }

        $writer->close();

        $footer = FloeFileContext::footer($filesystem, $path);

        static::assertSame(1, $footer->totalRows);
        static::assertCount(1, $footer->sections);
    }

    public function test_append_with_new_non_nullable_column_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://evolve.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path);
        $writer->write(rows(row(int_entry('id', 1))));
        $writer->close();

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('adds new column "email" which must be nullable');

        $writer = new FloeWriter($filesystem);
        $writer->append($path);
        $writer->write(rows(row(int_entry('id', 2), str_entry('email', 'x'))));
    }

    public function test_append_with_new_nullable_column_merges_file_schema(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://merge.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path);
        $writer->write(rows(row(int_entry('id', 1))));
        $writer->close();

        $writer = new FloeWriter($filesystem);
        $writer->append($path);
        $writer->write(rows(row(int_entry('id', 2), str_entry('email', null))));
        $writer->close();

        $footer = FloeFileContext::footer($filesystem, $path);
        $email = $footer->fileSchema()->findDefinition('email');

        static::assertSame(2, $footer->totalRows);
        static::assertCount(2, $footer->schemas);
        static::assertCount(2, $footer->sections);
        static::assertNotNull($email);
        static::assertTrue($email->isNullable());
    }

    public function test_append_with_same_schema_starts_section_without_schema_frame(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://same-schema.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path);
        $writer->write(rows(row(int_entry('id', 1))));
        $writer->close();

        $writer = new FloeWriter($filesystem);
        $writer->append($path);
        $writer->write(rows(row(int_entry('id', 2))));
        $writer->close();

        static::assertSame(
            [Format::FRAME_SCHEMA, Format::FRAME_ROW, Format::FRAME_FOOTER, Format::FRAME_ROW, Format::FRAME_FOOTER],
            FloeFileContext::frameTypes($filesystem, $path),
        );

        $footer = FloeFileContext::footer($filesystem, $path);

        static::assertSame(2, $footer->totalRows);
        static::assertCount(1, $footer->schemas);
        static::assertCount(2, $footer->sections);
        static::assertSame($footer->sections[0]->schemaId, $footer->sections[1]->schemaId);
    }

    public function test_close_twice_throws(): void
    {
        $writer = new FloeWriter(memory_filesystem());
        $writer->create(path('memory://closed.floe'));
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is not open');

        $writer->close();
    }

    public function test_opening_a_second_session_throws(): void
    {
        $writer = new FloeWriter(memory_filesystem());
        $writer->create(path('memory://first.floe'));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is already open');

        $writer->create(path('memory://second.floe'));
    }

    public function test_closing_empty_file_writes_header_and_footer_only(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://empty.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path);
        $writer->close();

        static::assertSame([Format::FRAME_FOOTER], FloeFileContext::frameTypes($filesystem, $path));

        $footer = FloeFileContext::footer($filesystem, $path);

        static::assertSame(0, $footer->totalRows);
        static::assertSame([], $footer->sections);
        static::assertSame([], $footer->schemas);
        static::assertSame([], $footer->fileSchema);
        static::assertSame([], $footer->partitions);
    }

    public function test_create_with_non_noop_codec_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('supports only the no-op codec, got codec 0x05');

        (new FloeWriter(memory_filesystem(), new CodecStub(0x05)))->create(path('memory://codec.floe'));
    }

    public function test_open_on_stream_is_byte_identical_to_create(): void
    {
        $filesystem = memory_filesystem();
        $viaCreate = path('memory://via-create.floe');
        $viaStream = path('memory://via-stream.floe');
        $data = rows(row(int_entry('id', 1), str_entry('name', 'a')), row(int_entry('id', 2), str_entry('name', 'b')));

        $create = new FloeWriter($filesystem);
        $create->create($viaCreate);
        $create->write($data);
        $create->close();

        $onStream = new FloeWriter($filesystem);
        $onStream->createOnStream($filesystem->writeTo($viaStream));
        $onStream->write($data);
        $onStream->close();

        static::assertSame($filesystem->readFrom($viaCreate)->content(), $filesystem->readFrom($viaStream)->content());
    }

    public function test_open_on_stream_round_trips(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://on-stream.floe');

        $writer = new FloeWriter($filesystem);
        $writer->createOnStream($filesystem->writeTo($path), Metadata::fromArray(['source' => 'stream']));
        $writer->write(rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3))));
        $writer->close();

        $footer = FloeFileContext::footer($filesystem, $path);

        static::assertSame(3, $footer->totalRows);
        static::assertSame(['source' => 'stream'], $footer->metadata->normalize());
        static::assertSame(
            [Format::FRAME_SCHEMA, Format::FRAME_ROW, Format::FRAME_ROW, Format::FRAME_ROW, Format::FRAME_FOOTER],
            FloeFileContext::frameTypes($filesystem, $path),
        );
    }

    public function test_open_on_stream_with_non_noop_codec_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('supports only the no-op codec, got codec 0x05');

        (new FloeWriter(memory_filesystem(), new CodecStub(0x05)))->createOnStream(memory_filesystem()->writeTo(path(
            'memory://on-stream-codec.floe',
        )));
    }

    public function test_nothing_reaches_the_stream_before_buffer_fills_or_close(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://buffered.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path);
        $writer->write(rows(row(str_entry('data', 'x'))));

        static::assertSame(0, $filesystem->readFrom($path)->size());

        $writer->write(rows(row(str_entry('data', str_repeat('x', 70_000)))));

        static::assertGreaterThan(0, $filesystem->readFrom($path)->size());
    }

    public function test_partitioned_rows_write_partitions_frame_after_header(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://partitioned.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path);
        $writer->write(Rows::partitioned([row(int_entry('id', 1), str_entry('country', 'PL'))], [new Partition(
            'country',
            'PL',
        )]));
        $writer->close();

        static::assertSame(
            [Format::FRAME_PARTITIONS, Format::FRAME_SCHEMA, Format::FRAME_ROW, Format::FRAME_FOOTER],
            FloeFileContext::frameTypes($filesystem, $path),
        );
        static::assertSame(['country' => 'PL'], FloeFileContext::footer($filesystem, $path)->partitions);
    }

    public function test_partitions_fixed_empty_reject_partitioned_writes(): void
    {
        $writer = new FloeWriter(memory_filesystem());
        $writer->create(path('memory://fixed-empty.floe'));
        $writer->write(rows(row(int_entry('id', 1))));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('one partition combination');

        $writer->write(Rows::partitioned([row(int_entry('id', 2), str_entry('country', 'PL'))], [new Partition(
            'country',
            'PL',
        )]));
    }

    public function test_partitions_mismatch_on_append_throws(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://append-partitions.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path);
        $writer->write(Rows::partitioned([row(int_entry('id', 1), str_entry('country', 'PL'))], [new Partition(
            'country',
            'PL',
        )]));
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('one partition combination');

        $writer = new FloeWriter($filesystem);
        $writer->append($path);
        $writer->write(Rows::partitioned([row(int_entry('id', 2), str_entry('country', 'US'))], [new Partition(
            'country',
            'US',
        )]));
    }

    public function test_a_new_column_grows_a_section_and_narrower_rows_ride_it(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://sections.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path);
        $writer->write(rows(
            // section {a,b}; then c is new -> grow to section {a,b,c}; then {a,b} fits it (c absent)
            row(int_entry('a', 1), str_entry('b', 'x')),
            row(int_entry('a', 2), str_entry('c', 'y')),
            row(int_entry('a', 3), str_entry('b', 'z')),
        ));
        $writer->close();

        $footer = FloeFileContext::footer($filesystem, $path);

        static::assertCount(2, $footer->schemas);
        static::assertCount(2, $footer->sections);
        static::assertSame([0, 1], [
            $footer->sections[0]->schemaId,
            $footer->sections[1]->schemaId,
        ]);
        static::assertSame([1, 2], [
            $footer->sections[0]->rowCount,
            $footer->sections[1]->rowCount,
        ]);
        static::assertSame(3, $footer->totalRows);
    }

    public function test_section_offsets_point_at_frames(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offsets.floe');

        $writer = new FloeWriter($filesystem);
        $writer->create($path);
        $writer->write(rows(row(int_entry('a', 1)), row(str_entry('b', 'x'))));
        $writer->close();

        $footer = FloeFileContext::footer($filesystem, $path);
        $source = $filesystem->readFrom($path);

        static::assertSame(Format::HEADER_LENGTH, $footer->sections[0]->offset);

        foreach ($footer->sections as $section) {
            static::assertSame(Format::FRAME_SCHEMA, ord($source->read(1, $section->offset)));
        }
    }

    public function test_write_after_close_throws(): void
    {
        $writer = new FloeWriter(memory_filesystem());
        $writer->create(path('memory://done.floe'));
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is not open');

        $writer->write(rows(row(int_entry('id', 1))));
    }

    public function test_grow_section_plan_from_null_uses_the_row_schema(): void
    {
        $plan = FloeWriter::growSectionPlan(null, row(int_entry('a', 1), str_entry('b', 'x')));

        static::assertSame(['a', 'b'], array_values(array_map(static fn($column) => $column->name, $plan->columns)));
    }

    public function test_grow_section_plan_grows_the_union_preserving_order(): void
    {
        $base = FloeWriter::growSectionPlan(null, row(int_entry('a', 1), str_entry('b', 'x')));

        $grown = FloeWriter::growSectionPlan($base->schemaBody, row(int_entry('a', 2), float_entry('c', 1.5)));

        static::assertSame(
            ['a', 'b', 'c'],
            array_values(array_map(static fn($column) => $column->name, $grown->columns)),
        );
    }

    public function test_grow_section_plan_rides_a_narrower_row_without_growing(): void
    {
        $base = FloeWriter::growSectionPlan(null, row(int_entry('a', 1), str_entry('b', 'x')));

        $grown = FloeWriter::growSectionPlan($base->schemaBody, row(int_entry('a', 2)));

        static::assertSame($base->schemaBody, $grown->schemaBody);
    }
}
