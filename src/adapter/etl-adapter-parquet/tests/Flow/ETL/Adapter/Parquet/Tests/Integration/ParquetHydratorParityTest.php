<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use Flow\ETL\Adapter\Parquet\ParquetEncoder;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Reader;

use function array_map;
use function file_exists;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function serialize;
use function unlink;

/**
 * Guards the Parquet read path: the trusted (no-cast) read - reader values normalized to Flow-native
 * by ParquetEncoder::decode, then hydrated without casting - must be byte-identical to the previous
 * casting read (AdaptiveRowHydrator over the raw reader values). If the reader ever returns a value the
 * casting read fixed but decode does not, this fails loudly.
 */
final class ParquetHydratorParityTest extends FlowTestCase
{
    private string $path = __DIR__ . '/var/hydrator_parity.parquet';

    protected function tearDown(): void
    {
        if (file_exists($this->path)) {
            unlink($this->path);
        }
    }

    public function test_trusted_read_is_identical_to_a_casting_read(): void
    {
        data_frame(config())
            ->read(new FakeExtractor(50))
            ->drop('null', 'enum')
            ->mode(overwrite())
            ->write(to_parquet(path($this->path)))
            ->run();

        $file = (new Reader())->read($this->path);
        $flowSchema = (new SchemaConverter())->toFlow($file->schema());
        $rawRows = iterator_to_array($file->values(), false);

        $encoder = new ParquetEncoder($file->schema());
        $decodedRows = $encoder->decode($rawRows);

        $trusted = (new AdaptiveRowHydrator())->hydrate($decodedRows, $flowSchema);
        $cast = (new AdaptiveRowHydrator())->cast(
            array_map(static fn(array $values): RawRowValues => new RawRowValues($values), $rawRows),
            $flowSchema,
        );

        static::assertSame(serialize($cast), serialize($trusted));
    }
}
