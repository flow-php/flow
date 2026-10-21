<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Composer\InstalledVersions;
use Flow\Filesystem\DestinationStream;
use Flow\Parquet\Dremel\DremelShredder;
use Flow\Parquet\Dremel\Validator\ColumnDataValidator;
use Flow\Parquet\Dremel\Validator\DisabledValidator;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Index\IndexLocation;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFile\RowGroups;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileWriter;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\Thrift\PhpFileStream;
use Flow\Parquet\ThriftModel\ColumnIndex as ThriftColumnIndex;
use Flow\Parquet\ThriftModel\OffsetIndex as ThriftOffsetIndex;
use Flow\Parquet\Writer\ColumnLists;
use Flow\Parquet\Writer\RowGroupBuilder;

use function array_chunk;
use function array_map;
use function array_slice;
use function array_values;
use function fclose;
use function fopen;
use function is_array;
use function max;
use function min;
use function pack;
use function stream_get_contents;
use function strlen;

final class PhpParquetFileWriter implements ParquetFileWriter
{
    private int $fileOffset;

    private readonly Metadata $metadata;

    private readonly RowGroupBuilder $rowGroupBuilder;

    /**
     * @var list<list<?ColumnIndex>>
     */
    private array $columnIndexes = [];

    /**
     * @var list<list<OffsetIndex>>
     */
    private array $offsetIndexes = [];

    /**
     * Written row groups, added to the metadata once their page indexes are written after the last of them.
     *
     * @var list<RowGroup>
     */
    private array $rowGroups = [];

    private ?DestinationStream $stream;

    public function __construct(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        private readonly int $rowGroupSizeCheckInterval,
    ) {
        $this->metadata = new Metadata(
            $schema,
            new RowGroups([]),
            0,
            $options->getInt(Option::WRITER_VERSION),
            'flow-php parquet version ' . InstalledVersions::getRootPackage()['pretty_version'],
            array_map(ColumnOrder::forWriting(...), array_values($schema->columnsFlat())),
        );
        $this->rowGroupBuilder = new RowGroupBuilder(
            $schema,
            $compression,
            $options,
            new DremelShredder(
                $options->getBool(Option::VALIDATE_DATA) ? new ColumnDataValidator() : new DisabledValidator(),
                DataConverter::initialize($options),
            ),
        );
        $stream->append(ParquetFile::PARQUET_MAGIC_NUMBER);
        $this->stream = $stream;
        $this->fileOffset = strlen(ParquetFile::PARQUET_MAGIC_NUMBER);
    }

    public function close(): void
    {
        $stream = $this->stream ?? throw new RuntimeException('Writer is not open');

        try {
            if (!$this->rowGroupBuilder->isEmpty()) {
                $this->flushRowGroup($stream);
            }

            $this->writePageIndexes($stream);

            $metadataHandle = fopen('php://temp/maxmemory:' . (5 * 1024 * 1024), 'rb+');

            if ($metadataHandle === false) {
                throw new RuntimeException('Cannot open temporary stream');
            }

            $this->metadata->toThrift()->write(new CompactProtocol(new PhpFileStream($metadataHandle)));
            $metadataBytes = stream_get_contents($metadataHandle, offset: 0);

            if ($metadataBytes === false) {
                throw new RuntimeException('Cannot read metadata from temporary stream');
            }

            $stream->append($metadataBytes);
            fclose($metadataHandle);

            $stream->append(pack('l', strlen($metadataBytes)));
            $stream->append(ParquetFile::PARQUET_MAGIC_NUMBER);
            $stream->close();
        } finally {
            $this->stream = null;
        }
    }

    public function writeBatch(iterable $rows): void
    {
        $stream = $this->stream ?? throw new RuntimeException('Writer is not open');

        if (!is_array($rows)) {
            foreach ($rows as $row) {
                $this->writeRow($row);
            }

            return;
        }

        /** @var int<1, max> $interval */
        $interval = $this->rowGroupSizeCheckInterval;

        foreach (array_chunk($rows, $interval) as $chunk) {
            $this->rowGroupBuilder->addRows($chunk);

            if ($this->rowGroupBuilder->isFull()) {
                $this->flushRowGroup($stream);
            }
        }
    }

    public function writeColumns(array $columns): void
    {
        $stream = $this->stream ?? throw new RuntimeException('Writer is not open');
        $count = (new ColumnLists())->length($columns);

        /** @var int<1, max> $interval */
        $interval = $this->rowGroupSizeCheckInterval;

        for ($offset = 0; $offset < $count; $offset += $interval) {
            $length = min($interval, $count - $offset);
            $slices = [];

            foreach ($columns as $name => $values) {
                $slices[$name] = array_slice($values, $offset, $length);
            }

            $this->rowGroupBuilder->addColumns($slices, $length);

            if ($this->rowGroupBuilder->isFull()) {
                $this->flushRowGroup($stream);
            }
        }
    }

    public function writeRow(array $row): void
    {
        $stream = $this->stream ?? throw new RuntimeException('Writer is not open');
        $this->rowGroupBuilder->addRow($row);

        if (
            ($this->rowGroupBuilder->rowsCount() % $this->rowGroupSizeCheckInterval) === 0
            && $this->rowGroupBuilder->isFull()
        ) {
            $this->flushRowGroup($stream);
        }
    }

    private function flushRowGroup(DestinationStream $stream): void
    {
        $rowGroupContainer = $this->rowGroupBuilder->flush($this->fileOffset);
        $stream->append($rowGroupContainer->binaryBuffer);
        $this->rowGroups[] = $rowGroupContainer->rowGroup;
        $this->columnIndexes[] = $rowGroupContainer->columnIndexes;
        $this->offsetIndexes[] = $rowGroupContainer->offsetIndexes;
        $this->fileOffset += strlen($rowGroupContainer->binaryBuffer);
    }

    /**
     * Every column index, then every offset index, between the last row group and the footer (parquet.thrift
     * PageIndex layout); each chunk is given its indexes' locations.
     */
    private function writePageIndexes(DestinationStream $stream): void
    {
        $columnIndexLocations = [];
        $offsetIndexLocations = [];

        foreach ($this->columnIndexes as $rowGroup => $columnIndexes) {
            foreach ($columnIndexes as $chunk => $columnIndex) {
                $columnIndexLocations[$rowGroup][$chunk] = $columnIndex === null
                    ? null
                    : $this->writeThrift($stream, $columnIndex->toThrift());
            }
        }

        foreach ($this->offsetIndexes as $rowGroup => $offsetIndexes) {
            foreach ($offsetIndexes as $chunk => $offsetIndex) {
                $offsetIndexLocations[$rowGroup][$chunk] = $this->writeThrift($stream, $offsetIndex->toThrift());
            }
        }

        foreach ($this->rowGroups as $rowGroup => $group) {
            $chunks = [];

            foreach (array_values($group->columnChunks()) as $chunk => $columnChunk) {
                $chunks[] = $columnChunk->withLocations(
                    columnIndex: $columnIndexLocations[$rowGroup][$chunk] ?? null,
                    offsetIndex: $offsetIndexLocations[$rowGroup][$chunk],
                );
            }

            $this->metadata->rowGroups()->add(new RowGroup($chunks, $group->rowsCount()));
        }
    }

    private function writeThrift(DestinationStream $stream, ThriftColumnIndex|ThriftOffsetIndex $struct): IndexLocation
    {
        $struct->write(new CompactProtocol($buffer = new MemoryBuffer()));
        $bytes = $buffer->data();
        $stream->append($bytes);
        $location = new IndexLocation($this->fileOffset, strlen($bytes));
        $this->fileOffset += strlen($bytes);

        return $location;
    }
}
