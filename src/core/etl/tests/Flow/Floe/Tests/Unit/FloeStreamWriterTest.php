<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Rows;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Partition;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\FloeStreamWriter;
use Flow\Floe\FloeWriter;
use Flow\Floe\Format;
use Flow\Floe\Options;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use Flow\Floe\Tests\Double\CodecStub;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
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
        $data = rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'a']),
            row(['id' => 2, 'name' => 'b']),
        );
        $schema = $data->schema();

        $create = new FloeWriter($filesystem, $schema);
        $create->create($viaCreate);
        $create->write($data);
        $create->close();

        $onStream = new FloeStreamWriter($schema);
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
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'alpha']),
            row(['id' => 2, 'name' => 'beta']),
            row(['id' => 3, 'name' => 'gamma']),
        );

        $schema = $data->schema();

        $defaultWriter = new FloeStreamWriter($schema);
        $defaultWriter->create($filesystem->writeTo($default));
        $defaultWriter->write($data);
        $defaultWriter->close();

        $tinyWriter = new FloeStreamWriter($schema, new Options(bufferSize: 4));
        $tinyWriter->create($filesystem->writeTo($tiny));
        $tinyWriter->write($data);
        $tinyWriter->close();

        static::assertSame($filesystem->readFrom($default)->content(), $filesystem->readFrom($tiny)->content());
    }

    public function test_create_on_stream_round_trips(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://on-stream.floe');

        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));

        $writer = new FloeStreamWriter($data->schema());
        $writer->create($filesystem->writeTo($path), Metadata::fromArray(['source' => 'stream']));
        $writer->write($data);
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(3, $footer->totalRows);
        static::assertSame(['source' => 'stream'], $footer->metadata->normalize());
        static::assertSame(
            [Format::FRAME_ROW, Format::FRAME_ROW, Format::FRAME_ROW, Format::FRAME_FOOTER],
            FloeStreamReaderContext::frameTypes($filesystem, $path),
        );
    }

    public function test_construct_with_non_noop_codec_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('supports only the no-op codec, got codec 0x05');

        new FloeStreamWriter(schema(), new Options(codec: new CodecStub(0x05)));
    }

    public function test_creating_a_second_session_throws(): void
    {
        $writer = new FloeStreamWriter(schema());
        $writer->create(memory_filesystem()->writeTo(path('memory://first.floe')));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is already open');

        $writer->create(memory_filesystem()->writeTo(path('memory://second.floe')));
    }

    public function test_write_before_create_throws(): void
    {
        $writer = new FloeStreamWriter(schema());

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is not open');

        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));
    }

    public function test_aligned_multi_batch_session_is_byte_identical_to_a_single_batch(): void
    {
        $filesystem = memory_filesystem();
        $multi = path('memory://multi-batch.floe');
        $single = path('memory://single-batch.floe');

        $data = rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'a']),
            row(['id' => 2, 'name' => 'b']),
        );
        $schema = $data->schema();

        $multiWriter = new FloeStreamWriter($schema);
        $multiWriter->create($filesystem->writeTo($multi));
        $multiWriter->write(rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'a'])));
        $multiWriter->write(rows(schema(int_schema('id'), str_schema('name')), row(['id' => 2, 'name' => 'b'])));
        $multiWriter->close();

        $singleWriter = new FloeStreamWriter($schema);
        $singleWriter->create($filesystem->writeTo($single));
        $singleWriter->write($data);
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

        $first = rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'a']));

        $writer = new FloeStreamWriter($first->schema());
        $writer->create($filesystem->writeTo($path));
        $writer->write($first);
        $writer->write(rows(schema(int_schema('id')), row(['id' => 2])));
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);
        static::assertSame(2, $footer->totalRows);
        static::assertNotSame([], $footer->schema);
        static::assertCount(2, FloeStreamReaderContext::readAll($filesystem, $path)->all());
    }

    public function test_batch_introducing_a_new_column_throws_naming_the_column(): void
    {
        $writer = new FloeStreamWriter(schema(int_schema('id')));
        $writer->create(memory_filesystem()->writeTo(path('memory://new-column.floe')));
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('new column "email"');

        $writer->write(rows(schema(int_schema('id'), str_schema('email')), row(['id' => 2, 'email' => 'x'])));
    }

    public function test_batch_with_an_incompatible_type_throws_naming_the_column(): void
    {
        $writer = new FloeStreamWriter(schema(int_schema('id')));
        $writer->create(memory_filesystem()->writeTo(path('memory://type-drift.floe')));
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('column "id"');

        $writer->write(rows(schema(str_schema('id')), row(['id' => 'x'])));
    }

    public function test_new_column_message_names_the_session_schema_and_the_column(): void
    {
        $writer = new FloeStreamWriter(schema(int_schema('id')));
        $writer->create(memory_filesystem()->writeTo(path('memory://hint.floe')));
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage(
            'Floe write session schema is fixed and this batch does not fit it: new column "email".',
        );

        $writer->write(rows(schema(int_schema('id'), str_schema('email')), row(['id' => 2, 'email' => 'x'])));
    }

    public function test_explicit_session_schema_is_used_for_the_footer(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://explicit-schema.floe');

        $writer = new FloeStreamWriter(schema(int_schema('id'), str_schema('name')));
        $writer->create($filesystem->writeTo($path));
        $writer->write(rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'a'])));
        $writer->close();

        $fileSchema = FloeStreamReaderContext::footer($filesystem, $path)->schema();
        static::assertNotNull($fileSchema->findDefinition('id'));
        static::assertNotNull($fileSchema->findDefinition('name'));
    }

    public function test_empty_batch_writes_no_schema_or_section(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://empty.floe');

        $writer = new FloeStreamWriter(schema());
        $writer->create($filesystem->writeTo($path));
        $writer->write(rows(schema()));
        $writer->close();

        static::assertSame([Format::FRAME_FOOTER], FloeStreamReaderContext::frameTypes($filesystem, $path));

        $footer = FloeStreamReaderContext::footer($filesystem, $path);
        static::assertSame(0, $footer->totalRows);
        static::assertSame([], $footer->schema);
        static::assertCount(0, $footer->sections);
    }

    public function test_writing_a_column_name_with_invalid_utf8_throws(): void
    {
        $badRows = rows(schema(str_schema('badÿname')), row(['badÿname' => 'x']));

        $writer = new FloeStreamWriter($badRows->schema());
        $writer->create(memory_filesystem()->writeTo(path('memory://bad-name.floe')));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('failed to encode schema as JSON');

        // the schema is stored only in the footer, so the pure PHP engine rejects it when the footer
        // is written, while the native engine rejects it earlier, when it encodes the batch
        $writer->write($badRows);
        $writer->close();
    }

    public function test_validation_off_is_byte_identical_to_validation_on_for_a_fitting_batch(): void
    {
        $filesystem = memory_filesystem();
        $on = path('memory://validate-on.floe');
        $off = path('memory://validate-off.floe');
        $data = rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'a']),
            row(['id' => 2, 'name' => 'b']),
        );
        $schema = $data->schema();

        $onWriter = new FloeStreamWriter($schema);
        $onWriter->create($filesystem->writeTo($on));
        $onWriter->write($data);
        $onWriter->close();

        $offWriter = new FloeStreamWriter($schema, new Options(validateData: false));
        $offWriter->create($filesystem->writeTo($off));
        $offWriter->write($data);
        $offWriter->close();

        static::assertSame($filesystem->readFrom($on)->content(), $filesystem->readFrom($off)->content());
    }

    public function test_validation_off_multi_batch_is_byte_identical_to_single_batch(): void
    {
        $filesystem = memory_filesystem();
        $multi = path('memory://off-multi.floe');
        $single = path('memory://off-single.floe');
        $data = rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'a']),
            row(['id' => 2, 'name' => 'b']),
        );
        $schema = $data->schema();

        $multiWriter = new FloeStreamWriter($schema, new Options(validateData: false));
        $multiWriter->create($filesystem->writeTo($multi));
        $multiWriter->write(rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'a'])));
        $multiWriter->write(rows(schema(int_schema('id'), str_schema('name')), row(['id' => 2, 'name' => 'b'])));
        $multiWriter->close();

        $singleWriter = new FloeStreamWriter($schema, new Options(validateData: false));
        $singleWriter->create($filesystem->writeTo($single));
        $singleWriter->write($data);
        $singleWriter->close();

        static::assertSame($filesystem->readFrom($single)->content(), $filesystem->readFrom($multi)->content());
    }

    public function test_validation_off_encodes_a_later_null_into_a_non_nullable_session_column(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://off-present-then-null.floe');
        $first = rows(schema(int_schema('id'), str_schema('opt')), row(['id' => 1, 'opt' => 'present']));

        $writer = new FloeStreamWriter($first->schema(), new Options(validateData: false));
        $writer->create($filesystem->writeTo($path));
        $writer->write($first);
        $writer->write(rows(
            schema(int_schema('id'), str_schema('opt', nullable: true)),
            row(['id' => 2, 'opt' => null]),
        ));
        $writer->close();

        static::assertSame(
            [['id' => 1, 'opt' => 'present'], ['id' => 2, 'opt' => null]],
            FloeStreamReaderContext::readAll($filesystem, $path)->toArray(),
        );
    }

    public function test_validation_on_rejects_a_later_null_into_a_non_nullable_session_column(): void
    {
        $first = rows(schema(int_schema('id'), str_schema('opt')), row(['id' => 1, 'opt' => 'present']));

        $writer = new FloeStreamWriter($first->schema());
        $writer->create(memory_filesystem()->writeTo(path('memory://on-present-then-null.floe')));
        $writer->write($first);

        $this->expectException(IncompatibleSchemaException::class);

        $writer->write(rows(
            schema(int_schema('id'), str_schema('opt', nullable: true)),
            row(['id' => 2, 'opt' => null]),
        ));
    }

    /**
     * SCHEMA EVOLUTION: pins that an undeclared column is ALWAYS rejected. If adding an optional
     * column becomes legal, this test states the rule that has to change - a mandatory column
     * must still be rejected.
     */
    public function test_column_absent_from_the_session_schema_throws_even_with_validation_off(): void
    {
        $writer = new FloeStreamWriter(schema(int_schema('a')), new Options(validateData: false));
        $writer->create(memory_filesystem()->writeTo(path('memory://off-new-column.floe')));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('new column "b"');

        $writer->write(rows(schema(int_schema('a'), int_schema('b')), row(['a' => 1, 'b' => 2])));
    }

    public function test_rejected_batch_registers_no_partition_combination(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://rejected-partitions.floe');

        $writer = new FloeStreamWriter(schema(int_schema('id')));
        $writer->create($filesystem->writeTo($path));
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1])));

        try {
            $writer->write(Rows::partitioned(
                schema(str_schema('id')),
                [row(['id' => 'AB-1'])],
                [new Partition('g', 'x')],
            ));
        } catch (IncompatibleSchemaException) {
        }

        $writer->close();

        static::assertSame([[]], FloeStreamReaderContext::footer($filesystem, $path)->partitions);
    }

    public function test_long_string_value_is_truncated_in_the_error_message(): void
    {
        $writer = new FloeStreamWriter(schema(int_schema('id')));
        $writer->create(memory_filesystem()->writeTo(path('memory://truncated.floe')));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage("could not convert '" . str_repeat('x', 32) . "...' (string) to integer");

        $writer->write(rows(schema(str_schema('id')), row(['id' => str_repeat('x', 40)])));
    }

    public function test_boolean_value_is_rendered_unquoted_in_the_error_message(): void
    {
        $writer = new FloeStreamWriter(schema(int_schema('id')));
        $writer->create(memory_filesystem()->writeTo(path('memory://bool-message.floe')));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('could not convert true (boolean) to integer');

        $writer->write(rows(schema(bool_schema('id')), row(['id' => true])));
    }
}
