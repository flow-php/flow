<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\FloeStreamWriter;
use Flow\Floe\FloeWriter;
use Flow\Floe\Format;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use Flow\Floe\Tests\Double\CodecStub;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FloeStreamWriterTest extends TestCase
{
    public function test_create_on_stream_is_byte_identical_to_path_create(): void
    {
        $filesystem = memory_filesystem();
        $viaCreate = path('memory://via-create.floe');
        $viaStream = path('memory://via-stream.floe');
        $data = rows(row(int_entry('id', 1), str_entry('name', 'a')), row(int_entry('id', 2), str_entry('name', 'b')));

        $create = new FloeWriter($filesystem);
        $create->create($viaCreate);
        $create->write($data);
        $create->close();

        $onStream = new FloeStreamWriter();
        $onStream->create($filesystem->writeTo($viaStream));
        $onStream->write($data);
        $onStream->close();

        static::assertSame($filesystem->readFrom($viaCreate)->content(), $filesystem->readFrom($viaStream)->content());
    }

    public function test_small_buffer_size_produces_byte_identical_output(): void
    {
        $filesystem = memory_filesystem();
        $default = path('memory://default-buffer.floe');
        $tiny = path('memory://tiny-buffer.floe');
        $data = rows(
            row(int_entry('id', 1), str_entry('name', 'alpha')),
            row(int_entry('id', 2), str_entry('name', 'beta')),
            row(int_entry('id', 3), str_entry('name', 'gamma')),
        );

        $defaultWriter = new FloeStreamWriter();
        $defaultWriter->create($filesystem->writeTo($default));
        $defaultWriter->write($data);
        $defaultWriter->close();

        $tinyWriter = new FloeStreamWriter(bufferSize: 4);
        $tinyWriter->create($filesystem->writeTo($tiny));
        $tinyWriter->write($data);
        $tinyWriter->close();

        static::assertSame($filesystem->readFrom($default)->content(), $filesystem->readFrom($tiny)->content());
    }

    public function test_create_on_stream_round_trips(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://on-stream.floe');

        $writer = new FloeStreamWriter();
        $writer->create($filesystem->writeTo($path), Metadata::fromArray(['source' => 'stream']));
        $writer->write(rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3))));
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(3, $footer->totalRows);
        static::assertSame(['source' => 'stream'], $footer->metadata->normalize());
        static::assertSame(
            [Format::FRAME_SCHEMA, Format::FRAME_ROW, Format::FRAME_ROW, Format::FRAME_ROW, Format::FRAME_FOOTER],
            FloeStreamReaderContext::frameTypes($filesystem, $path),
        );
    }

    public function test_construct_with_non_noop_codec_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('supports only the no-op codec, got codec 0x05');

        new FloeStreamWriter(new CodecStub(0x05));
    }

    public function test_creating_a_second_session_throws(): void
    {
        $writer = new FloeStreamWriter();
        $writer->create(memory_filesystem()->writeTo(path('memory://first.floe')));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is already open');

        $writer->create(memory_filesystem()->writeTo(path('memory://second.floe')));
    }

    public function test_write_before_create_throws(): void
    {
        $writer = new FloeStreamWriter();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is not open');

        $writer->write(rows(row(int_entry('id', 1))));
    }

    public function test_aligned_multi_batch_session_is_byte_identical_to_a_single_batch(): void
    {
        $filesystem = memory_filesystem();
        $multi = path('memory://multi-batch.floe');
        $single = path('memory://single-batch.floe');

        $multiWriter = new FloeStreamWriter();
        $multiWriter->create($filesystem->writeTo($multi));
        $multiWriter->write(rows(row(int_entry('id', 1), str_entry('name', 'a'))));
        $multiWriter->write(rows(row(int_entry('id', 2), str_entry('name', 'b'))));
        $multiWriter->close();

        $singleWriter = new FloeStreamWriter();
        $singleWriter->create($filesystem->writeTo($single));
        $singleWriter->write(rows(
            row(int_entry('id', 1), str_entry('name', 'a')),
            row(int_entry('id', 2), str_entry('name', 'b')),
        ));
        $singleWriter->close();

        static::assertSame($filesystem->readFrom($single)->content(), $filesystem->readFrom($multi)->content());

        $footer = FloeStreamReaderContext::footer($filesystem, $multi);
        static::assertNotSame([], $footer->schema);
        static::assertCount(1, $footer->sections);
    }

    public function test_subset_column_batch_is_accepted_and_round_trips(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://subset.floe');

        $writer = new FloeStreamWriter();
        $writer->create($filesystem->writeTo($path));
        $writer->write(rows(row(int_entry('id', 1), str_entry('name', 'a'))));
        $writer->write(rows(row(int_entry('id', 2))));
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);
        static::assertSame(2, $footer->totalRows);
        static::assertNotSame([], $footer->schema);
        static::assertCount(2, FloeStreamReaderContext::readAll($filesystem, $path)->all());
    }

    public function test_batch_introducing_a_new_column_throws_naming_the_column(): void
    {
        $writer = new FloeStreamWriter();
        $writer->create(memory_filesystem()->writeTo(path('memory://new-column.floe')));
        $writer->write(rows(row(int_entry('id', 1))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('new column "email"');

        $writer->write(rows(row(int_entry('id', 2), str_entry('email', 'x'))));
    }

    public function test_batch_with_an_incompatible_type_throws_naming_the_column(): void
    {
        $writer = new FloeStreamWriter();
        $writer->create(memory_filesystem()->writeTo(path('memory://type-drift.floe')));
        $writer->write(rows(row(int_entry('id', 1))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('column "id"');

        $writer->write(rows(row(str_entry('id', 'x'))));
    }

    public function test_new_column_message_hints_at_data_frame_match(): void
    {
        $writer = new FloeStreamWriter();
        $writer->create(memory_filesystem()->writeTo(path('memory://hint.floe')));
        $writer->write(rows(row(int_entry('id', 1))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('DataFrame::match');

        $writer->write(rows(row(int_entry('id', 2), str_entry('email', 'x'))));
    }

    public function test_explicit_session_schema_is_used_for_the_footer(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://explicit-schema.floe');

        $writer = new FloeStreamWriter();
        $writer->create($filesystem->writeTo($path), schema: schema(int_schema('id'), str_schema('name')));
        $writer->write(rows(row(int_entry('id', 1), str_entry('name', 'a'))));
        $writer->close();

        $fileSchema = FloeStreamReaderContext::footer($filesystem, $path)->schema();
        static::assertNotNull($fileSchema->findDefinition('id'));
        static::assertNotNull($fileSchema->findDefinition('name'));
    }

    public function test_empty_batch_writes_no_schema_or_section(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://empty.floe');

        $writer = new FloeStreamWriter();
        $writer->create($filesystem->writeTo($path));
        $writer->write(rows());
        $writer->close();

        static::assertSame([Format::FRAME_FOOTER], FloeStreamReaderContext::frameTypes($filesystem, $path));

        $footer = FloeStreamReaderContext::footer($filesystem, $path);
        static::assertSame(0, $footer->totalRows);
        static::assertSame([], $footer->schema);
        static::assertCount(0, $footer->sections);
    }

    public function test_writing_a_column_name_with_invalid_utf8_throws(): void
    {
        $writer = new FloeStreamWriter();
        $writer->create(memory_filesystem()->writeTo(path('memory://bad-name.floe')));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('failed to encode schema as JSON');

        $writer->write(rows(row(str_entry("bad\xFFname", 'x'))));
    }
}
