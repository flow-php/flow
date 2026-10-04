<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\OffsetOverflow;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeStreamWriter;
use Flow\Floe\FloeWriter;
use Flow\Floe\Format;
use Flow\Floe\Options;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use Flow\Floe\Tests\Double\CodecStub;
use Flow\Floe\Tests\Double\OverflowingColumnStub;
use PHPUnit\Framework\TestCase;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class FloeStreamWriterTest extends TestCase
{
    public function test_create_on_stream_is_byte_identical_to_path_create(): void
    {
        $filesystem = memory_filesystem();
        $viaCreate = path('memory://via-create.floe');
        $viaStream = path('memory://via-stream.floe');
        $data = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']],
            schema(int_schema('id'), str_schema('name')),
        );
        $schema = $data->schema();

        $create = new FloeWriter($filesystem, $schema, new AdaptiveBackend());
        $create->create($viaCreate);
        $create->write($data);
        $create->close();

        $onStream = new FloeStreamWriter($schema, new AdaptiveBackend());
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
        $data = array_to_rows(
            [['id' => 1, 'name' => 'alpha'], ['id' => 2, 'name' => 'beta'], ['id' => 3, 'name' => 'gamma']],
            schema(int_schema('id'), str_schema('name')),
        );

        $schema = $data->schema();

        $defaultWriter = new FloeStreamWriter($schema, new AdaptiveBackend());
        $defaultWriter->create($filesystem->writeTo($default));
        $defaultWriter->write($data);
        $defaultWriter->close();

        $tinyWriter = new FloeStreamWriter($schema, new AdaptiveBackend(), new Options(bufferSize: 4));
        $tinyWriter->create($filesystem->writeTo($tiny));
        $tinyWriter->write($data);
        $tinyWriter->close();

        static::assertSame($filesystem->readFrom($default)->content(), $filesystem->readFrom($tiny)->content());
    }

    public function test_create_on_stream_round_trips(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://on-stream.floe');

        $data = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        $writer = new FloeStreamWriter($data->schema(), new AdaptiveBackend());
        $writer->create($filesystem->writeTo($path), Metadata::fromArray(['source' => 'stream']));
        $writer->write($data);
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);

        static::assertSame(3, $footer->statistics->rows);
        static::assertSame(['source' => 'stream'], $footer->metadata->normalize());
        static::assertSame(
            [Format::FRAME_BATCH, Format::FRAME_FOOTER],
            FloeStreamReaderContext::frameTypes($filesystem, $path),
        );
    }

    public function test_construct_with_non_noop_codec_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('supports only the no-op codec, got codec 0x05');

        new FloeStreamWriter(schema(), new AdaptiveBackend(), new Options(codec: new CodecStub(0x05)));
    }

    public function test_creating_a_second_session_throws(): void
    {
        $writer = new FloeStreamWriter(schema(), new AdaptiveBackend());
        $writer->create(memory_filesystem()->writeTo(path('memory://first.floe')));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is already open');

        $writer->create(memory_filesystem()->writeTo(path('memory://second.floe')));
    }

    public function test_write_before_create_throws(): void
    {
        $writer = new FloeStreamWriter(schema(), new AdaptiveBackend());

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe writer session is not open');

        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
    }

    public function test_a_multi_batch_session_reads_back_the_rows_of_a_single_batch(): void
    {
        $filesystem = memory_filesystem();
        $multi = path('memory://multi-batch.floe');
        $single = path('memory://single-batch.floe');

        $data = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']],
            schema(int_schema('id'), str_schema('name')),
        );
        $schema = $data->schema();

        $multiWriter = new FloeStreamWriter($schema, new AdaptiveBackend());
        $multiWriter->create($filesystem->writeTo($multi));
        $multiWriter->write(array_to_rows([['id' => 1, 'name' => 'a']], schema(int_schema('id'), str_schema('name'))));
        $multiWriter->write(array_to_rows([['id' => 2, 'name' => 'b']], schema(int_schema('id'), str_schema('name'))));
        $multiWriter->close();

        $singleWriter = new FloeStreamWriter($schema, new AdaptiveBackend());
        $singleWriter->create($filesystem->writeTo($single));
        $singleWriter->write($data);
        $singleWriter->close();

        static::assertSame(
            [Format::FRAME_BATCH, Format::FRAME_BATCH, Format::FRAME_FOOTER],
            FloeStreamReaderContext::frameTypes($filesystem, $multi),
        );
        static::assertSame(
            (new FloeReader($filesystem, new AdaptiveBackend()))
                ->read($single)
                ->rows()
                ->current()
                ->toArray(),
            array_merge(...array_map(
                static fn(Rows $batch) => $batch->toArray(),
                iterator_to_array(
                    (new FloeReader($filesystem, new AdaptiveBackend()))
                        ->read($multi)
                        ->rows(),
                    false,
                ),
            )),
        );

        $footer = FloeStreamReaderContext::footer($filesystem, $multi);
        static::assertNotSame([], $footer->schema);
        static::assertCount(1, $footer->sections);
    }

    public function test_batch_omitting_a_nullable_session_column_is_accepted_and_round_trips(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://subset.floe');

        $first = array_to_rows(
            [['id' => 1, 'name' => 'a']],
            schema(int_schema('id'), str_schema('name', nullable: true)),
        );

        $writer = new FloeStreamWriter($first->schema(), new AdaptiveBackend());
        $writer->create($filesystem->writeTo($path));
        $writer->write($first);
        $writer->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
        $writer->close();

        $footer = FloeStreamReaderContext::footer($filesystem, $path);
        static::assertSame(2, $footer->statistics->rows);
        static::assertNotSame([], $footer->schema);
        static::assertCount(2, FloeStreamReaderContext::readAll($filesystem, $path));
    }

    /**
     * b57, writer half: the old check walked the row's own values, so a column the row simply
     * omitted was never looked at and was encoded as VALUE_ABSENT under a NOT NULL declaration.
     */
    public function test_batch_omitting_a_not_null_session_column_is_refused(): void
    {
        $first = array_to_rows([['id' => 1, 'name' => 'a']], schema(int_schema('id'), str_schema('name')));

        $writer = new FloeStreamWriter($first->schema(), new AdaptiveBackend());
        $writer->create(memory_filesystem()->writeTo(path('memory://subset-not-null.floe')));
        $writer->write($first);

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('Missing Definitions');

        $writer->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
    }

    public function test_batch_introducing_a_new_column_throws_naming_the_column(): void
    {
        $writer = new FloeStreamWriter(schema(int_schema('id')), new AdaptiveBackend());
        $writer->create(memory_filesystem()->writeTo(path('memory://new-column.floe')));
        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('new column "email"');

        $writer->write(array_to_rows([['id' => 2, 'email' => 'x']], schema(int_schema('id'), str_schema('email'))));
    }

    public function test_batch_with_an_incompatible_type_throws_naming_the_column(): void
    {
        $writer = new FloeStreamWriter(schema(int_schema('id')), new AdaptiveBackend());
        $writer->create(memory_filesystem()->writeTo(path('memory://type-drift.floe')));
        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('expected: id<integer>, given: id<string>');

        $writer->write(array_to_rows([['id' => 'x']], schema(str_schema('id'))));
    }

    public function test_new_column_message_names_the_session_schema_and_the_column(): void
    {
        $writer = new FloeStreamWriter(schema(int_schema('id')), new AdaptiveBackend());
        $writer->create(memory_filesystem()->writeTo(path('memory://hint.floe')));
        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage(
            'Floe write session schema is fixed and this batch does not fit it: new column "email".',
        );

        $writer->write(array_to_rows([['id' => 2, 'email' => 'x']], schema(int_schema('id'), str_schema('email'))));
    }

    public function test_explicit_session_schema_is_used_for_the_footer(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://explicit-schema.floe');

        $writer = new FloeStreamWriter(schema(int_schema('id'), str_schema('name')), new AdaptiveBackend());
        $writer->create($filesystem->writeTo($path));
        $writer->write(array_to_rows([['id' => 1, 'name' => 'a']], schema(int_schema('id'), str_schema('name'))));
        $writer->close();

        $fileSchema = FloeStreamReaderContext::footer($filesystem, $path)->schema();
        static::assertNotNull($fileSchema->findDefinition('id'));
        static::assertNotNull($fileSchema->findDefinition('name'));
    }

    public function test_empty_batch_writes_no_schema_or_section(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://empty.floe');

        $writer = new FloeStreamWriter(schema(), new AdaptiveBackend());
        $writer->create($filesystem->writeTo($path));
        $writer->write(rows(schema()));
        $writer->close();

        static::assertSame([Format::FRAME_FOOTER], FloeStreamReaderContext::frameTypes($filesystem, $path));

        $footer = FloeStreamReaderContext::footer($filesystem, $path);
        static::assertSame(0, $footer->statistics->rows);
        static::assertSame([], $footer->schema);
        static::assertCount(0, $footer->sections);
    }

    public function test_writing_a_column_name_with_invalid_utf8_throws(): void
    {
        $badRows = array_to_rows([['badÿname' => 'x']], schema(str_schema('badÿname')));

        $writer = new FloeStreamWriter($badRows->schema(), new AdaptiveBackend());
        $writer->create(memory_filesystem()->writeTo(path('memory://bad-name.floe')));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('failed to encode schema as JSON');

        // the schema is stored only in the footer, so the pure PHP engine rejects it when the footer
        // is written, while the native engine rejects it earlier, when it encodes the batch
        $writer->write($badRows);
        $writer->close();
    }

    public function test_a_later_null_into_a_non_nullable_session_column_is_rejected(): void
    {
        $first = array_to_rows([['id' => 1, 'opt' => 'present']], schema(int_schema('id'), str_schema('opt')));

        $writer = new FloeStreamWriter($first->schema(), new AdaptiveBackend());
        $writer->create(memory_filesystem()->writeTo(path('memory://off-present-then-null.floe')));
        $writer->write($first);

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('expected: opt<string>, given: opt<?string>');

        $writer->write(array_to_rows(
            [['id' => 2, 'opt' => null]],
            schema(int_schema('id'), str_schema('opt', nullable: true)),
        ));
    }

    public function test_validation_on_rejects_a_later_null_into_a_non_nullable_session_column(): void
    {
        $first = array_to_rows([['id' => 1, 'opt' => 'present']], schema(int_schema('id'), str_schema('opt')));

        $writer = new FloeStreamWriter($first->schema(), new AdaptiveBackend());
        $writer->create(memory_filesystem()->writeTo(path('memory://on-present-then-null.floe')));
        $writer->write($first);

        $this->expectException(IncompatibleSchemaException::class);

        $writer->write(array_to_rows(
            [['id' => 2, 'opt' => null]],
            schema(int_schema('id'), str_schema('opt', nullable: true)),
        ));
    }

    /**
     * SCHEMA EVOLUTION: pins that an undeclared column is ALWAYS rejected. If adding an optional
     * column becomes legal, this test states the rule that has to change - a mandatory column
     * must still be rejected.
     */
    public function test_column_absent_from_the_session_schema_throws(): void
    {
        $writer = new FloeStreamWriter(schema(int_schema('a')), new AdaptiveBackend());
        $writer->create(memory_filesystem()->writeTo(path('memory://off-new-column.floe')));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('new column "b"');

        $writer->write(array_to_rows([['a' => 1, 'b' => 2]], schema(int_schema('a'), int_schema('b'))));
    }

    public function test_an_overflowing_batch_is_split_in_halves(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://split.floe');
        $schema = schema(int_schema('id'), str_schema('name'));
        $data = array_to_rows([
            ['id' => 1, 'name' => 'a'],
            ['id' => 2, 'name' => 'b'],
            ['id' => 3, 'name' => 'c'],
            ['id' => 4, 'name' => 'd'],
        ], $schema);
        $rows = Rows::fromColumns(
            $schema,
            [
                'id' => $data->column('id'),
                'name' => new OverflowingColumnStub($data->column('name'), new OffsetOverflow('overflow'), maxRows: 2),
            ],
            4,
        );

        $writer = new FloeStreamWriter($schema, new AdaptiveBackend());
        $writer->create($filesystem->writeTo($path));
        $writer->write($rows);
        $writer->close();

        static::assertSame(
            [Format::FRAME_BATCH, Format::FRAME_BATCH, Format::FRAME_FOOTER],
            FloeStreamReaderContext::frameTypes($filesystem, $path),
        );
        static::assertSame(
            [[1, 2], [3, 4]],
            array_map(
                static fn(Rows $batch) => $batch->reduceToArray('id'),
                iterator_to_array(
                    (new FloeReader($filesystem, new AdaptiveBackend()))
                        ->read($path)
                        ->rows(),
                    false,
                ),
            ),
        );
    }

    public function test_a_corrupt_column_fails_on_the_first_encode(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(str_schema('name'));
        $name = new OverflowingColumnStub(
            array_to_rows([['name' => 'a'], ['name' => 'b']], $schema)->column('name'),
            new InvalidArgumentException('corrupt offsets'),
            maxRows: 0,
        );

        $writer = new FloeStreamWriter($schema, new AdaptiveBackend());
        $writer->create($filesystem->writeTo(path('memory://corrupt.floe')));

        try {
            $writer->write(Rows::fromColumns($schema, ['name' => $name], 2));
            static::fail('A corrupt column must fail the write');
        } catch (InvalidArgumentException $e) {
            static::assertSame('corrupt offsets', $e->getMessage());
            static::assertSame(1, $name->encodeCalls);
        }
    }

    public function test_a_single_overflowing_row_rethrows_offset_overflow(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(str_schema('name'));
        $name = new OverflowingColumnStub(
            array_to_rows([['name' => 'a']], $schema)->column('name'),
            new OffsetOverflow('overflow'),
            maxRows: 0,
        );

        $writer = new FloeStreamWriter($schema, new AdaptiveBackend());
        $writer->create($filesystem->writeTo(path('memory://single.floe')));

        $this->expectException(OffsetOverflow::class);
        $this->expectExceptionMessage('overflow (row 0)');

        $writer->write(Rows::fromColumns($schema, ['name' => $name], 1));
    }
}
