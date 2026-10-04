<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFileWriter;
use Flow\Parquet\Reader;

use function array_keys;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

/**
 * The same rows through ParquetFileWriter::writeBatch() and ::writeColumns().
 */
final class ColumnDoor
{
    /**
     * @param array<array<array-key, mixed>> $rows every row with the keys of the first
     *
     * @return array<string, list<mixed>>
     */
    public static function columns(array $rows): array
    {
        $columns = [];

        foreach (array_keys($rows[0] ?? []) as $name) {
            $columns[(string) $name] = [];
        }

        foreach ($rows as $row) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($row as $name => $value) {
                $columns[(string) $name][] = $value;
            }
        }

        return $columns;
    }

    public static function open(
        ParquetEngine $engine,
        StringDestinationStream $stream,
        Schema $schema,
        Options $options = new Options(),
    ): ParquetFileWriter {
        return $engine->openForWrite($stream, $schema, Compressions::UNCOMPRESSED, $options);
    }

    /**
     * @return list<array<string, mixed>> what the PHP reader reads back from $stream
     */
    public static function read(StringDestinationStream $stream): array
    {
        /** @var non-empty-string $bytes */
        $bytes = $stream->content();

        return iterator_to_array(MemoryParquetFile::read(Reader::php(), $bytes)->values(), false);
    }

    /**
     * A flat, a list, a map and a structure column; every third value is null.
     *
     * @return list<array<string, mixed>>
     */
    public static function rows(int $count): array
    {
        $rows = [];

        for ($i = 0; $i < $count; $i++) {
            $rows[] = [
                'id' => $i,
                'name' => ($i % 3) === 0 ? null : 'name-' . $i,
                'tags' => ($i % 3) === 1 ? null : ['t' . $i, 'u' . $i],
                'counts' => ($i % 3) === 2 ? null : ['a' => $i, 'b' => $i + 1],
                'point' => ($i % 3) === 0 ? null : ['x' => $i, 'y' => ($i % 2) === 0 ? null : $i * 2],
            ];
        }

        return $rows;
    }

    public static function schema(): Schema
    {
        return Schema::with(
            FlatColumn::int64('id'),
            FlatColumn::string('name'),
            NestedColumn::list('tags', ListElement::string()),
            NestedColumn::map('counts', MapKey::string(), MapValue::int32()),
            NestedColumn::structure('point', [FlatColumn::int32('x'), FlatColumn::int32('y')]),
        );
    }

    public static function stream(): StringDestinationStream
    {
        return new StringDestinationStream(path('memory://file.parquet'));
    }
}
