<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row\Hydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Schema\Metadata;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeStreamReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\PhpFloeEncoder;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use Flow\Floe\Tests\Double\ClosingSpySourceStream;
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

final class FloeStreamReaderTest extends TestCase
{
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
