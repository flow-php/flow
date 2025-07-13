<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\{Dremel\WriteColumnData,
    Option,
    Options,
    Writer\ColumnChunkBuilder\DeltaBinaryPackedColumnChunkBuilder,
    Writer\ColumnChunkBuilder\NestedColumnChunkBuilder,
    Writer\ColumnChunkBuilder\PlainFlatColumnChunkBuilder,
    Writer\ColumnChunkBuilder\RLEDictionaryChunkBuilder};
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\{Compressions, Encodings, Schema};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, NestedColumn, PhysicalType};

final class ColumnChunkBuilders
{
    /**
     * @param array<string, ColumnChunkBuilder> $builders
     */
    public function __construct(
        private array $builders,
    ) {
    }

    public static function initialize(Schema $schema, Options $options, Compressions $compressions) : self
    {
        $builders = [];

        foreach ($schema->columns() as $column) {
            if ($column instanceof NestedColumn) {
                $builders[$column->name()] = new NestedColumnChunkBuilder(
                    $column,
                    array_map(
                        fn (FlatColumn $childColumn) => self::createFlatColumnBuilder($childColumn, $options, $compressions),
                        $column->childrenFlat()
                    )
                );
            } else {
                /** @var FlatColumn $column */
                $builders[$column->name()] = self::createFlatColumnBuilder($column, $options, $compressions);
            }
        }

        return new self($builders);
    }

    public function add(WriteColumnData $columnData) : void
    {
        $this->builders[$columnData->column->name()]->addRow($columnData);
    }

    /**
     * Close all pages in the column chunk builders.
     */
    public function closePages() : void
    {
        foreach ($this->builders as $builder) {
            $builder->closePage();
        }
    }

    /**
     * @return array<ColumnChunkContainer>
     */
    public function flush(int $fileOffset) : array
    {
        $offset = $fileOffset;
        $containers = [];

        foreach ($this->builders as $builder) {
            foreach ($builder->flush($offset) as $container) {
                $containers[] = $container;
                $offset += \strlen($container->binaryBuffer);
            }
        }

        return $containers;
    }

    /**
     * Check if any of the column chunk builders has reached the maximum page size.
     */
    public function isAnyPageFull() : bool
    {
        foreach ($this->builders as $builder) {
            if ($builder->isFull()) {
                return true;
            }
        }

        return false;
    }

    public function uncompressedSize() : int
    {
        $size = 0;

        foreach ($this->builders as $builder) {
            $size += $builder->uncompressedSize();
        }

        return $size;
    }

    private static function createBuilderForEncoding(FlatColumn $column, Encodings $encoding, Options $options, Compressions $compressions) : ColumnChunkBuilder
    {
        self::validateEncodingForColumn($column, $encoding);

        return match ($encoding) {
            Encodings::PLAIN => new PlainFlatColumnChunkBuilder($column, $options, $compressions),
            Encodings::RLE_DICTIONARY => new RLEDictionaryChunkBuilder($column, $options, $compressions),
            Encodings::DELTA_BINARY_PACKED => new DeltaBinaryPackedColumnChunkBuilder($column, $options, $compressions),
            default => throw new InvalidArgumentException("Unsupported encoding for column builder: {$encoding->name}"),
        };
    }

    private static function createFlatColumnBuilder(FlatColumn $column, Options $options, Compressions $compressions) : ColumnChunkBuilder
    {
        if ($options->has(Option::COLUMNS_ENCODINGS)) {
            $columnsEncodings = $options->getColumnsEncodings();
            $flatPath = $column->flatPath();

            if ($columnsEncodings !== null && $columnsEncodings->hasFlatPath($flatPath)) {
                $encoding = $columnsEncodings->getEncodingForFlatPath($flatPath);

                if ($encoding !== null) {
                    return self::createBuilderForEncoding($column, $encoding, $options, $compressions);
                }
            }
        }

        if (($column->type() === PhysicalType::INT32 || $column->type() === PhysicalType::INT64) && $options->getInt(Option::WRITER_VERSION) === 2) {
            return new DeltaBinaryPackedColumnChunkBuilder($column, $options, $compressions);
        }

        return new PlainFlatColumnChunkBuilder($column, $options, $compressions);
    }

    private static function validateEncodingForColumn(FlatColumn $column, Encodings $encoding) : void
    {
        $columnType = $column->type();
        $encodingName = $encoding->name;
        $flatPath = $column->flatPath();

        switch ($encoding) {
            case Encodings::DELTA_BINARY_PACKED:
                if ($columnType !== PhysicalType::INT32 && $columnType !== PhysicalType::INT64) {
                    throw new InvalidArgumentException(
                        'DELTA_BINARY_PACKED encoding is only supported for INT32 and INT64 columns. ' .
                        "Column '{$flatPath}' has type: {$columnType->name}"
                    );
                }

                break;

            case Encodings::RLE_DICTIONARY:
            case Encodings::PLAIN:
                break;

            default:
                throw new InvalidArgumentException(
                    "Encoding '{$encodingName}' is not implemented. " .
                    'Supported encodings: PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED'
                );
        }
    }
}
