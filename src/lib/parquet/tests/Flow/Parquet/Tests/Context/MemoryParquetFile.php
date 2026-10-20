<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\Reader;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\ThriftModel\FileMetaData;
use Flow\Parquet\Writer;

use function array_column;
use function array_map;
use function array_values;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function pack;
use function range;
use function strlen;
use function substr;
use function unpack;

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
     * The values of one column of the file, read through $reader.
     *
     * @param non-empty-string $bytes
     *
     * @return list<mixed>
     */
    public static function column(Reader $reader, string $bytes, string $column): array
    {
        return array_values(array_column(iterator_to_array(self::read($reader, $bytes)->values(), false), $column));
    }

    /**
     * A string column `s`, RLE_DICTIONARY encoded, two values per page.
     *
     * @param list<?string> $values
     *
     * @return non-empty-string
     */
    public static function dictionaryColumn(array $values, Options $options): string
    {
        return self::written(
            Writer::php(options: $options->set(Option::PAGE_SIZE_BYTES, 8)->set(
                Option::PAGE_SIZE_CHECK_INTERVAL,
                2,
            )->set(Option::COLUMNS_ENCODINGS, ['s' => Encodings::RLE_DICTIONARY])),
            Schema::with(FlatColumn::string('s')),
            array_map(static fn(?string $value): array => ['s' => $value], $values),
        );
    }

    /**
     * One row group of 12 rows, two rows per page: `id` (INT64 0-11, PLAIN), `word` (RLE_DICTIONARY, "w{id % 3}") and
     * `tags` (a list of id % 3 + 1 ints, `id` repeated).
     *
     * @return non-empty-string
     */
    public static function pagedColumns(): string
    {
        return self::written(
            Writer::php(options: (new Options())->set(Option::PAGE_SIZE_BYTES, 16)->set(
                Option::PAGE_SIZE_CHECK_INTERVAL,
                2,
            )->set(Option::COLUMNS_ENCODINGS, ['word' => Encodings::RLE_DICTIONARY])),
            Schema::with(
                FlatColumn::int64('id'),
                FlatColumn::string('word'),
                NestedColumn::list('tags', ListElement::int64()),
            ),
            array_map(
                static fn(int $id): array => [
                    'id' => $id,
                    'word' => 'w' . ($id % 3),
                    'tags' => array_map(static fn(): int => $id, range(0, $id % 3)),
                ],
                range(0, 11),
            ),
        );
    }

    /**
     * The file with every column chunk recording a bloom filter length but no offset.
     *
     * @param non-empty-string $bytes
     *
     * @return non-empty-string
     */
    public static function bloomFilterLengthWithoutOffset(string $bytes): string
    {
        $footerLength = unpack('V', substr($bytes, -8, 4))[1];
        $metadata = new FileMetaData();
        $metadata->read(new CompactProtocol(new MemoryBuffer(substr($bytes, -8 - $footerLength, $footerLength))));

        foreach ($metadata->row_groups as $rowGroup) {
            foreach ($rowGroup->columns as $column) {
                $chunk = $column->meta_data ?? throw new RuntimeException('No column metadata');
                $chunk->bloom_filter_offset = null;
                $chunk->bloom_filter_length = 48;
            }
        }

        $metadata->write(new CompactProtocol($buffer = new MemoryBuffer()));

        return substr($bytes, 0, -8 - $footerLength) . $buffer->data() . pack('V', strlen($buffer->data())) . 'PAR1';
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
