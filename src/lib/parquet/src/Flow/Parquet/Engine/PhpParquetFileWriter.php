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
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\RowGroups;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileWriter;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\PhpFileStream;
use Flow\Parquet\Writer\RowGroupBuilder;

use function array_chunk;
use function fclose;
use function fopen;
use function is_array;
use function pack;
use function stream_get_contents;
use function strlen;

final class PhpParquetFileWriter implements ParquetFileWriter
{
    private int $fileOffset;

    private readonly Metadata $metadata;

    private readonly RowGroupBuilder $rowGroupBuilder;

    private ?DestinationStream $stream;

    public function __construct(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        private readonly int $rowGroupSizeCheckInterval,
    ) {
        $stream->append(ParquetFile::PARQUET_MAGIC_NUMBER);
        $this->stream = $stream;
        $this->fileOffset = strlen(ParquetFile::PARQUET_MAGIC_NUMBER);
        $this->metadata = new Metadata(
            $schema,
            new RowGroups([]),
            0,
            $options->getInt(Option::WRITER_VERSION),
            'flow-php parquet version ' . InstalledVersions::getRootPackage()['pretty_version'],
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
    }

    public function close(): void
    {
        $stream = $this->stream ?? throw new RuntimeException('Writer is not open');

        try {
            if (!$this->rowGroupBuilder->isEmpty()) {
                $this->flushRowGroup($stream);
            }

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
        $this->metadata->rowGroups()->add($rowGroupContainer->rowGroup);
        $this->fileOffset += strlen($rowGroupContainer->binaryBuffer);
    }
}
