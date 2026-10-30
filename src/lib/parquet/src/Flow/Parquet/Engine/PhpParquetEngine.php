<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\ParquetFileWriter;

use function array_key_exists;
use function is_array;
use function is_string;

/**
 * @type EngineConfiguration = array{byte_order: string, options: ParquetOptions}
 * @import-type ParquetOptions from Options
 *
 */
final class PhpParquetEngine implements ParquetEngine
{
    public function __construct(
        private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
        private readonly Options $options = new Options(),
    ) {}

    public static function fromArray(array $configuration): static
    {
        if (!array_key_exists('byte_order', $configuration) || !is_string($configuration['byte_order'])) {
            throw new InvalidArgumentException('Parquet engine configuration must hold a "byte_order" value');
        }

        if (!array_key_exists('options', $configuration) || !is_array($configuration['options'])) {
            throw new InvalidArgumentException('Parquet engine configuration must hold an "options" array');
        }

        return new self(
            ByteOrder::tryFrom($configuration['byte_order']) ?? throw new InvalidArgumentException(
                "Unknown byte order: {$configuration['byte_order']}",
            ),
            Options::fromArray($configuration['options']),
        );
    }

    /**
     * @return EngineConfiguration
     */
    public function normalize(): array
    {
        return ['byte_order' => $this->byteOrder->value, 'options' => $this->options->normalize()];
    }

    public function openForRead(SourceStream $stream): ParquetFileReader
    {
        return new PhpParquetFileReader($stream, $this->byteOrder, $this->options);
    }

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter {
        return new PhpParquetFileWriter(
            $stream,
            $schema,
            $compression,
            $options,
            $options->getInt(Option::ROW_GROUP_SIZE_CHECK_INTERVAL),
        );
    }

    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void {
        $file = new PhpParquetFileWriter(
            $stream,
            $schema,
            $compression,
            $options,
            $options->getInt(Option::ROW_GROUP_SIZE_CHECK_INTERVAL),
        );

        foreach ($rows as $row) {
            $file->writeRow($row);
        }

        $file->close();
    }
}
