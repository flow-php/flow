<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Codec;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\FloeMerger;
use Flow\Floe\FloeWriter;
use Flow\Floe\NativeFloeEncoder;
use Flow\Floe\Options;
use Flow\Floe\Tests\Context\FloeEngineContext;
use Flow\Floe\Tests\Double\PrefixingCodecStub;
use Flow\Floe\Tests\Mother\RowsMother;
use Override;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_reduce;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

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
            'empty' => [rows(schema())],
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

        $writer = new FloeWriter($this->fs(), $rows->schema());
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
            schema(int_schema('id'), str_schema('email', nullable: true)),
            row(['id' => 1]),
            row(['id' => 2]),
            row(['id' => 3]),
            row(['id' => 4, 'email' => null]),
            row(['id' => 5, 'email' => 'x']),
            row(['id' => 6]),
        );
        $writer = new FloeWriter($this->fs(), $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        static::assertEquals(
            iterator_to_array(FloeEngineContext::phpReader($this->fs())->read($path)->rows(1000, 2, 3)),
            iterator_to_array(FloeEngineContext::nativeReader($this->fs())->read($path)->rows(1000, 2, 3)),
        );
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

        $writer = new FloeWriter($this->fs(), $rows->schema(), new Options(codec: $codec));
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
                rows(schema(int_schema('id'), str_schema('email')), row(['id' => 1, 'email' => 'a'])),
                rows(schema(int_schema('id'), str_schema('email')), row(['id' => 2, 'email' => 'x'])),
            ],
        ];

        foreach ($cases as $name => $batches) {
            $schema = array_reduce(
                $batches,
                static fn(Schema $carry, Rows $batch): Schema => $carry->merge($batch->schema()),
                schema(),
            );

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

        $baseRows = rows(schema(int_schema('id')), row(['id' => 1]));
        $writer = new FloeWriter($this->fs(), $baseRows->schema());
        $writer->create($base);
        $writer->write($baseRows);
        $writer->close();

        $evolvedRows = rows(
            schema(int_schema('id'), str_schema('email', nullable: true)),
            row(['id' => 2, 'email' => null]),
        );
        $writer = new FloeWriter($this->fs(), $evolvedRows->schema());
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
