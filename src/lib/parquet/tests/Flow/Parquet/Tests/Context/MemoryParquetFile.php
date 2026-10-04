<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;

use function array_map;
use function Flow\Filesystem\DSL\path;
use function range;

/**
 * Parquet files written to and read from memory, never the filesystem.
 */
final class MemoryParquetFile
{
    /**
     * Rows 1-6 of `id` (INT64) and `tags` (a list of one string, "t{id}"), in three row groups of two rows.
     *
     * @return non-empty-string
     */
    public static function threeRowGroups(): string
    {
        return self::written(
            Writer::php(
                options: (new Options())
                    ->set(Option::ROW_GROUP_SIZE_BYTES, 1)
                    ->set(Option::ROW_GROUP_SIZE_CHECK_INTERVAL, 2)
                    ->set(Option::PAGE_SIZE_BYTES, 1)
                    ->set(Option::PAGE_SIZE_CHECK_INTERVAL, 1),
            ),
            Schema::with(FlatColumn::int64('id'), NestedColumn::list('tags', ListElement::string())),
            array_map(static fn(int $id): array => ['id' => $id, 'tags' => ['t' . $id]], range(1, 6)),
        );
    }

    /**
     * @param non-empty-string $bytes
     *
     * @return ParquetFile<ParquetFileReader>
     */
    public static function read(Reader $reader, string $bytes): ParquetFile
    {
        return $reader->readStream(new MemorySourceStream($bytes));
    }

    /**
     * @param iterable<array<array-key, mixed>> $rows
     *
     * @return non-empty-string
     */
    public static function written(Writer $writer, Schema $schema, iterable $rows): string
    {
        $stream = new StringDestinationStream(path('memory://file.parquet'));
        $writer->writeStream($stream, $schema, $rows);
        $content = $stream->content();

        return $content !== '' ? $content : throw new RuntimeException('Nothing was written');
    }
}
