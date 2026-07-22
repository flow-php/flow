<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Partition;
use Flow\Floe\Codec;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\FloeMerger;
use Flow\Floe\FloeStreamWriter;
use Flow\Floe\FloeWriter;
use Flow\Floe\Format;
use Flow\Floe\NativeFloeEncoder;
use Flow\Floe\Options;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\Tests\Context\FloeEngineContext;
use Flow\Floe\Tests\Context\FloeSchemaContext;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use Flow\Floe\Tests\Double\PrefixingCodecStub;
use Flow\Floe\Tests\Mother\RowsMother;
use Override;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_entry;
use function iterator_to_array;
use function pack;

/**
 * With the flow_php extension loaded, FloeReader hydrates ROW frame bodies
 * through the extension - these tests pin the extension path to the pure-PHP
 * hydrator, which stays the canonical behavior reference.
 */
final class FloeReaderExtensionParityTest extends FlowIntegrationTestCase
{
    public static function rows_datasets(): array
    {
        return [
            'all entry types' => [RowsMother::withAllEntryTypes()],
            'heterogeneous' => [RowsMother::heterogeneous()],
            'partitioned' => [RowsMother::partitioned()],
            'empty' => [rows()],
        ];
    }

    #[Override]
    protected function setUp(): void
    {
        parent::setUp();

        if (!NativeFloeEncoder::isSupported()) {
            self::markTestSkipped('flow_php extension with the RawRowValues pipeline is not loaded.');
        }
    }

    #[DataProvider('rows_datasets')]
    public function test_extension_and_pure_php_hydrate_identical_rows(Rows $rows): void
    {
        $path = $this->cacheDir->suffix('parity.floe');

        $writer = new FloeWriter($this->fs(), FloeStreamWriter::unionSchema($rows));
        $writer->create($path);
        $writer->write($rows);
        $writer->close();

        static::assertEquals(
            iterator_to_array(FloeEngineContext::phpReader($this->fs())->read($path)->rows()),
            iterator_to_array(FloeEngineContext::nativeReader($this->fs())->read($path)->rows()),
        );
    }

    public function test_extension_and_pure_php_seek_offset_identically(): void
    {
        $path = $this->cacheDir->suffix('parity-offset.floe');

        $data = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
            row(int_entry('id', 4), str_entry('email', null)),
            row(int_entry('id', 5), str_entry('email', 'x')),
            row(int_entry('id', 6)),
        );
        $writer = new FloeWriter($this->fs(), FloeStreamWriter::unionSchema($data));
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        static::assertEquals(
            iterator_to_array(FloeEngineContext::phpReader($this->fs())->read($path)->rows(1000, 2, 3)),
            iterator_to_array(FloeEngineContext::nativeReader($this->fs())->read($path)->rows(1000, 2, 3)),
        );
    }

    #[DataProvider('rows_datasets')]
    public function test_extension_and_pure_php_recover_identical_rows(Rows $rows): void
    {
        $path = $this->cacheDir->suffix('parity-recover.floe');

        $writer = new FloeWriter($this->fs(), FloeStreamWriter::unionSchema($rows));
        $writer->create($path);
        $writer->write($rows);
        $writer->close();

        static::assertEquals(
            iterator_to_array(FloeEngineContext::phpReader($this->fs())->read($path)->recover()),
            iterator_to_array(FloeEngineContext::nativeReader($this->fs())->read($path)->recover()),
        );
    }

    public function test_extension_and_pure_php_recover_a_torn_file_identically(): void
    {
        $path = $this->cacheDir->suffix('parity-recover-torn.floe');

        FloeStreamReaderContext::writeWithoutFooter(
            $this->fs(),
            $path,
            rows(row(int_entry('id', 1)), row(int_entry('id', 2), str_entry('email', 'x')), row(int_entry('id', 3))),
        );

        static::assertEquals(
            iterator_to_array(FloeEngineContext::phpReader($this->fs())->read($path)->recover()),
            iterator_to_array(FloeEngineContext::nativeReader($this->fs())->read($path)->recover()),
        );
    }

    public function test_extension_and_pure_php_salvage_rows_before_a_corrupt_row_identically(): void
    {
        $path = $this->cacheDir->suffix('parity-recover-corrupt.floe');

        $schemaBody = FloeSchemaContext::schemaBody(row(int_entry('id', 1))->schema());
        $encoder = new PhpFloeEncoder(schema_from_json($schemaBody));
        $hydrator = new PhpRowHydrator();

        $stream = $this->fs()->writeTo($path);
        $stream->append(
            Format::header(0x00)
                . Format::frame(Format::FRAME_SCHEMA, $schemaBody)
                . Format::frame(
                    Format::FRAME_ROW,
                    $encoder->encode($hydrator->dehydrate(rows(row(int_entry('id', 1)))))[0],
                )
                . Format::frame(
                    Format::FRAME_ROW,
                    $encoder->encode($hydrator->dehydrate(rows(row(int_entry('id', 2)))))[0],
                )
                . Format::frame(Format::FRAME_ROW, "\xEE"),
        );
        $stream->close();

        $pure = iterator_to_array(FloeEngineContext::phpReader($this->fs())->read($path)->recover());

        static::assertEquals(
            $pure,
            iterator_to_array(FloeEngineContext::nativeReader($this->fs())->read($path)->recover()),
        );
        static::assertCount(1, $pure);
        static::assertCount(2, $pure[0]->all());
    }

    public function test_extension_and_pure_php_recover_rows_around_a_late_partitions_frame_identically(): void
    {
        $path = $this->cacheDir->suffix('parity-recover-late-partitions.floe');

        $schemaBody = FloeSchemaContext::schemaBody(row(int_entry('id', 1))->schema());
        $encoder = new PhpFloeEncoder(schema_from_json($schemaBody));
        $hydrator = new PhpRowHydrator();
        $partitionsBody = pack('V', 1) . pack('V', 1) . 'g' . pack('V', 1) . 'a';

        $stream = $this->fs()->writeTo($path);
        $stream->append(
            Format::header(0x00)
                . Format::frame(Format::FRAME_SCHEMA, $schemaBody)
                . Format::frame(
                    Format::FRAME_ROW,
                    $encoder->encode($hydrator->dehydrate(rows(row(int_entry('id', 1)))))[0],
                )
                . Format::frame(Format::FRAME_PARTITIONS, $partitionsBody)
                . Format::frame(
                    Format::FRAME_ROW,
                    $encoder->encode($hydrator->dehydrate(rows(row(int_entry('id', 2)))))[0],
                ),
        );
        $stream->close();

        $pure = iterator_to_array(FloeEngineContext::phpReader($this->fs())->read($path)->recover());

        static::assertEquals(
            $pure,
            iterator_to_array(FloeEngineContext::nativeReader($this->fs())->read($path)->recover()),
        );
        // the partition change splits the rows into two batches so no batch mixes combinations
        static::assertCount(2, $pure);
        static::assertCount(1, $pure[0]->all());
        static::assertSame([], $pure[0]->partitions()->toArray());
        static::assertCount(1, $pure[1]->all());
        static::assertEquals([new Partition('g', 'a')], $pure[1]->partitions()->toArray());
    }

    /**
     * @return array<string, array{Codec}>
     */
    public static function codec_matrix(): array
    {
        return [
            'noop codec' => [new NoopCodec()],
            'transforming codec' => [new PrefixingCodecStub()],
        ];
    }

    #[DataProvider('rows_datasets')]
    public function test_extension_and_pure_php_read_identical_rows_with_transforming_codec(Rows $rows): void
    {
        $codec = new PrefixingCodecStub();
        $path = $this->cacheDir->suffix('read-codec.floe');

        $writer = new FloeWriter($this->fs(), FloeStreamWriter::unionSchema($rows), new Options(codec: $codec));
        $writer->create($path);
        $writer->write($rows);
        $writer->close();

        static::assertEquals(
            iterator_to_array(FloeEngineContext::phpReader($this->fs(), $codec)->read($path)->rows()),
            iterator_to_array(FloeEngineContext::nativeReader($this->fs(), $codec)->read($path)->rows()),
        );
    }

    /**
     * Full write->read parity across the engine switch for both codecs and for
     * plain, partitioned and schema-evolving files: a pure-PHP round-trip and a
     * native round-trip must yield identical rows in every cell.
     */
    #[DataProvider('codec_matrix')]
    public function test_round_trip_matrix_matches_across_engines(Codec $codec): void
    {
        $cases = [
            'plain' => [RowsMother::heterogeneous()],
            'partitioned' => [RowsMother::partitioned()],
            'multi-write' => [
                rows(row(int_entry('id', 1), str_entry('email', 'a'))),
                rows(row(int_entry('id', 2), str_entry('email', 'x'))),
            ],
        ];

        foreach ($cases as $name => $batches) {
            $allRows = new Rows();

            foreach ($batches as $batch) {
                $allRows = $allRows->merge($batch);
            }

            $schema = FloeStreamWriter::unionSchema($allRows);

            $purePath = $this->cacheDir->suffix("matrix-{$name}-pure.floe");
            $extPath = $this->cacheDir->suffix("matrix-{$name}-ext.floe");

            foreach ([[$purePath, false], [$extPath, true]] as [$path, $native]) {
                $writer = $native
                    ? FloeEngineContext::nativeWriter($this->fs(), $schema, $codec)
                    : FloeEngineContext::phpWriter($this->fs(), $schema, $codec);
                $writer->create($path);

                foreach ($batches as $batch) {
                    $writer->write($batch);
                }

                $writer->close();
            }

            static::assertSame(
                $this->fs()->readFrom($purePath)->content(),
                $this->fs()->readFrom($extPath)->content(),
                "byte-identity failed for {$name}",
            );

            static::assertEquals(
                iterator_to_array(FloeEngineContext::phpReader($this->fs(), $codec)->read($purePath)->rows()),
                iterator_to_array(FloeEngineContext::nativeReader($this->fs(), $codec)->read($extPath)->rows()),
                "row parity failed for {$name}",
            );
        }
    }

    public function test_extension_and_pure_php_pad_merged_sections_identically(): void
    {
        // a merged file re-encodes to one union schema; base rows lack email and must pad
        // identically across engines on read
        $path = $this->cacheDir->suffix('parity-evolved.floe');
        $base = $this->cacheDir->suffix('parity-base.floe');
        $evolved = $this->cacheDir->suffix('parity-new.floe');

        $baseRows = rows(row(int_entry('id', 1)));
        $writer = new FloeWriter($this->fs(), FloeStreamWriter::unionSchema($baseRows));
        $writer->create($base);
        $writer->write($baseRows);
        $writer->close();

        $evolvedRows = rows(row(int_entry('id', 2), str_entry('email', null)));
        $writer = new FloeWriter($this->fs(), FloeStreamWriter::unionSchema($evolvedRows));
        $writer->create($evolved);
        $writer->write($evolvedRows);
        $writer->close();

        (new FloeMerger($this->fs()))->merge([$base, $evolved], $path);

        static::assertEquals(
            iterator_to_array(FloeEngineContext::phpReader($this->fs())->read($path)->rows()),
            iterator_to_array(FloeEngineContext::nativeReader($this->fs())->read($path)->rows()),
        );
    }
}
