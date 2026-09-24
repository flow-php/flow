<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Closure;
use Flow\ETL\Row\Hydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\FloeEngine;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeStreamReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\NativeFloeEncoder;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use Flow\Floe\Tests\Double\ClosingSpySourceStream;
use Flow\Floe\Tests\Double\PrefixingCodecStub;
use Flow\Floe\Tests\Double\SpyHydrator;
use Flow\Floe\Tests\Mother\RowsMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_integer;
use function iterator_to_array;
use function range;
use function serialize;

final class FloeStreamReaderTest extends TestCase
{
    /**
     * @return array<string, array{Closure(FloeStreamReader): Generator, list<list<int>>}>
     */
    public static function reads(): array
    {
        return [
            'every row' => [
                static fn(FloeStreamReader $reader): Generator => $reader->rows(10),
                [range(1, 10), range(11, 20), range(21, 25)],
            ],
            'batch larger than the file' => [
                static fn(FloeStreamReader $reader): Generator => $reader->rows(100),
                [range(1, 25)],
            ],
            'limit below the batch size' => [
                static fn(FloeStreamReader $reader): Generator => $reader->rows(10, 0, 3),
                [range(1, 3)],
            ],
            'limit on a batch boundary' => [
                static fn(FloeStreamReader $reader): Generator => $reader->rows(10, 0, 20),
                [range(1, 10), range(11, 20)],
            ],
            'limit inside a batch' => [
                static fn(FloeStreamReader $reader): Generator => $reader->rows(10, 0, 15),
                [range(1, 10), range(11, 15)],
            ],
            'offset' => [
                static fn(FloeStreamReader $reader): Generator => $reader->rows(10, 4),
                [range(5, 14), range(15, 24), [25]],
            ],
            'offset and limit' => [
                static fn(FloeStreamReader $reader): Generator => $reader->rows(10, 4, 12),
                [range(5, 14), [15, 16]],
            ],
            'head' => [static fn(FloeStreamReader $reader): Generator => $reader->head(7, 5), [range(1, 5), [6, 7]]],
            'tail' => [
                static fn(FloeStreamReader $reader): Generator => $reader->tail(7, 5),
                [range(19, 23), [24, 25]],
            ],
        ];
    }

    public function test_close_closes_the_source_stream(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://close.floe');

        $data = rows(schema(int_schema('id')), row(['id' => 1]));
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $source = new ClosingSpySourceStream($filesystem->readFrom($path));

        (new FloeStreamReader($source, new NoopCodec(), 65_536))->close();

        static::assertSame(1, $source->closeCount);
    }

    /**
     * @param Closure(FloeStreamReader): Generator $read
     * @param list<list<int>> $batches
     */
    #[DataProvider('reads')]
    public function test_fused_read_yields_the_batches_of_the_two_step_read(Closure $read, array $batches): void
    {
        if (!NativeFloeEncoder::isSupported()) {
            static::markTestSkipped('flow_php extension with RustFloeEncoderNative is not loaded');
        }

        $filesystem = memory_filesystem();
        $path = path('memory://fused.floe');
        FloeStreamReaderContext::write($filesystem, $path, RowsMother::numbered(25));
        $spy = new SpyHydrator();

        $twoStep = iterator_to_array($read((new FloeReader($filesystem, hydrator: $spy))->read($path)));
        $fused = iterator_to_array($read((new FloeReader($filesystem))->read($path)));

        static::assertSame($batches, array_map(static fn(Rows $rows): array => $rows->reduceToArray('id'), $fused));
        static::assertEquals($twoStep, $fused);
        static::assertGreaterThan(0, $spy->hydrateCalls);
    }

    /**
     * @param Closure(FloeStreamReader): Generator $read
     * @param list<list<int>> $batches
     */
    #[DataProvider('reads')]
    public function test_native_engine_reads_a_transforming_codec_like_the_php_engine(
        Closure $read,
        array $batches,
    ): void {
        if (!NativeFloeEncoder::isSupported()) {
            static::markTestSkipped('flow_php extension with RustFloeEncoderNative is not loaded');
        }

        $filesystem = memory_filesystem();
        $codec = new PrefixingCodecStub();
        $path = path('memory://codec.floe');
        FloeStreamReaderContext::write($filesystem, $path, RowsMother::numbered(25), codec: $codec);

        $php = iterator_to_array($read((new FloeReader($filesystem, $codec, engine: FloeEngine::php))->read($path)));
        $native = iterator_to_array($read((new FloeReader($filesystem, $codec, engine: FloeEngine::native))->read(
            $path,
        )));

        static::assertEquals($php, $native);
        static::assertSame($batches, array_map(static fn(Rows $rows): array => $rows->reduceToArray('id'), $native));
    }

    /**
     * @param Closure(FloeStreamReader): Generator $read
     * @param list<list<int>> $batches
     */
    #[DataProvider('reads')]
    public function test_native_engine_reads_like_the_php_engine(Closure $read, array $batches): void
    {
        if (!NativeFloeEncoder::isSupported()) {
            static::markTestSkipped('flow_php extension with RustFloeEncoderNative is not loaded');
        }

        $filesystem = memory_filesystem();
        $path = path('memory://engines.floe');
        FloeStreamReaderContext::write($filesystem, $path, RowsMother::numbered(25));

        $php = iterator_to_array($read((new FloeReader($filesystem, engine: FloeEngine::php))->read($path)));
        $native = iterator_to_array($read((new FloeReader($filesystem, engine: FloeEngine::native))->read($path)));

        static::assertSame(serialize($php), serialize($native));
        static::assertSame($batches, array_map(static fn(Rows $rows): array => $rows->reduceToArray('id'), $native));
    }

    /**
     * @return array<string, array{null|Hydrator}>
     */
    public static function hydrators(): array
    {
        return ['default' => [null], 'php' => [new PhpRowHydrator()]];
    }

    #[DataProvider('hydrators')]
    public function test_batches_carry_the_file_schema_when_frames_carry_per_value_metadata(?Hydrator $hydrator): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://per-value-metadata.floe');
        $schema = schema(int_schema('id'));
        FloeStreamReaderContext::writeFrames($filesystem, $path, $schema, (new PhpFloeEncoder($schema))->encode([
            new TypedRowValues(['id' => 1], ['id' => type_integer()], ['id' => Metadata::fromArray(['k' => 'v'])]),
        ]));

        $batches = iterator_to_array(
            (new FloeReader($filesystem, hydrator: $hydrator))
                ->read($path)
                ->rows(),
        );

        static::assertCount(1, $batches);
        static::assertSame($schema->normalize(), $batches[0]->schema()->normalize());
        static::assertSame([1], $batches[0]->reduceToArray('id'));
    }
}
