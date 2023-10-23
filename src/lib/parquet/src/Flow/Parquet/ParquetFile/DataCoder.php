<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\ByteOrder;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\BitWidth;
use Flow\Parquet\ParquetFile\Data\RLEBitPackedHybrid;
use Flow\Parquet\ParquetFile\Page\ColumnData;
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class DataCoder
{
    public function __construct(
        private readonly Options $options,
        private readonly ByteOrder $byteOrder,
        private readonly LoggerInterface $logger = new NullLogger()
    ) {
    }

    public function decodeData(
        string $buffer,
        Encodings $encoding,
        PhysicalType $physicalType,
        LogicalType $logicalType = null,
        int $expectedValuesCount,
        int $maxRepetitionsLevel,
        int $maxDefinitionsLevel,
        ?int $typeLength = null,
        ?Dictionary $dictionary = null
    ) : ColumnData {
        $reader = new BinaryBufferReader($buffer, $this->byteOrder);
        $this->debugDecodeData($buffer, $encoding, $physicalType, $logicalType, $expectedValuesCount, $maxRepetitionsLevel, $maxDefinitionsLevel);

        $RLEBitPackedHybrid = new RLEBitPackedHybrid($this->logger);

        if ($maxRepetitionsLevel) {
            $this->debugLogRepetitions($maxRepetitionsLevel, $reader);
            $reader->readInt32();// read length of encoded data
            $repetitions = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($maxRepetitionsLevel),
                $expectedValuesCount,
            );
        } else {
            $repetitions = [];
        }

        if ($maxDefinitionsLevel) {
            $this->debugLogDefinitions($maxDefinitionsLevel, $reader);
            $reader->readInt32();// read length of encoded data
            $definitions = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($maxDefinitionsLevel),
                $expectedValuesCount,
            );
        } else {
            $definitions = [];
        }

        $nullsCount = \count($definitions) ? \count(\array_filter($definitions, fn ($definition) => $definition !== $maxDefinitionsLevel)) : 0;

        if ($encoding === Encodings::PLAIN) {
            $this->debugLogPlainEncoding($expectedValuesCount, $nullsCount);

            return new ColumnData(
                $physicalType,
                $logicalType,
                $repetitions,
                $definitions,
                $this->readPlainValues(
                    $physicalType,
                    $reader,
                    $expectedValuesCount - $nullsCount,
                    $logicalType,
                    $typeLength
                )
            );
        }

        if ($encoding === Encodings::RLE_DICTIONARY || $encoding === Encodings::PLAIN_DICTIONARY) {
            $this->debugLogDictionaryEncoding($expectedValuesCount, $nullsCount);

            if (\count($definitions)) {
                // while reading indices, there is no length at the beginning since length is simply a remaining length of the buffer
                // however we need to know bitWidth which is the first value in the buffer after definitions
                $bitWidth = $reader->readBytes(1)->toInt();
                /** @var array<int> $indices */
                $indices = $this->readRLEBitPackedHybrid(
                    $reader,
                    $RLEBitPackedHybrid,
                    $bitWidth,
                    $expectedValuesCount - $nullsCount,
                );

                /** @var array<mixed> $values */
                $values = [];

                foreach ($indices as $index) {
                    $values[] = $dictionary?->values[$index];
                }
            } else {
                $values = [];
            }

            return new ColumnData($physicalType, $logicalType, $repetitions, $definitions, $values);
        }

        throw new RuntimeException('Encoding ' . $encoding->name . ' not supported');
    }

    public function decodeDictionary(
        string $buffer,
        PhysicalType $physicalType,
        ?LogicalType $logicalType,
        Encodings $encoding,
        int $expectedValuesCount,
        ?int $typeLength = null
    ) : Dictionary {
        $reader = new BinaryBufferReader($buffer, $this->byteOrder);
        $this->debugLogDictionaryDecode($buffer, $encoding, $physicalType);

        return new Dictionary(
            $this->readPlainValues($physicalType, $reader, $expectedValuesCount, $logicalType, $typeLength)
        );
    }

    private function debugDecodeData(string $buffer, Encodings $encoding, PhysicalType $physicalType, ?LogicalType $logicalType, int $expectedValuesCount, int $maxRepetitionsLevel, int $maxDefinitionsLevel) : void
    {
        if ($this->logger instanceof NullLogger) {
            return;
        }

        $this->logger->debug('Decoding data', [
            'buffer_length' => \strlen($buffer),
            'encoding' => $encoding->name,
            'physical_type' => $physicalType->name,
            'logical_type' => $logicalType?->name(),
            'expected_values_count' => $expectedValuesCount,
            'max_repetitions_level' => $maxRepetitionsLevel,
            'max_definitions_level' => $maxDefinitionsLevel,
        ]);
    }

    private function debugLogDefinitions(int $maxDefinitionsLevel, BinaryBufferReader $reader) : void
    {
        if ($this->logger instanceof NullLogger) {
            return;
        }

        $this->logger->debug('Decoding definitions', ['max_definitions_level' => $maxDefinitionsLevel, 'reader_position' => ['bits' => $reader->position()->bits(), 'bytes' => $reader->position()->bytes()]]);
    }

    private function debugLogDictionaryDecode(string $buffer, Encodings $encoding, PhysicalType $physicalType) : void
    {
        if ($this->logger instanceof NullLogger) {
            return;
        }

        $this->logger->debug('Decoding dictionary', [
            'buffer_length' => \strlen($buffer),
            'encoding' => $encoding->name,
            'physical_type' => $physicalType->name,
        ]);
    }

    private function debugLogDictionaryEncoding(int $expectedValuesCount, int $nullsCount) : void
    {
        if ($this->logger instanceof NullLogger) {
            return;
        }

        $this->logger->debug('Decoding RLE_DICTIONARY/PLAIN_DICTIONARY values', ['not_nullable_values_count' => $expectedValuesCount - $nullsCount, 'nulls_count' => $nullsCount]);
    }

    private function debugLogPlainEncoding(int $expectedValuesCount, int $nullsCount) : void
    {
        if ($this->logger instanceof NullLogger) {
            return;
        }

        $this->logger->debug('Decoding PLAIN values', ['not_nullable_values_count' => $expectedValuesCount - $nullsCount, 'nulls_count' => $nullsCount]);
    }

    private function debugLogRepetitions(int $maxRepetitionsLevel, BinaryBufferReader $reader) : void
    {
        if ($this->logger instanceof NullLogger) {
            return;
        }

        $this->logger->debug('Decoding repetitions', ['max_repetitions_level' => $maxRepetitionsLevel, 'reader_position' => ['bits' => $reader->position()->bits(), 'bytes' => $reader->position()->bytes()]]);
    }

    private function debugLogRLEBitPackedHybridPost(array $data) : void
    {
        if ($this->logger instanceof NullLogger) {
            return;
        }

        $this->logger->debug('Decoded data', ['data_count' => \count($data), 'data' => $data]);
    }

    private function debugLogRLEBitPackedHybridPre(int $bitWidth, int $expectedValuesCount, BinaryBufferReader $reader) : void
    {
        if ($this->logger instanceof NullLogger) {
            return;
        }

        $this->logger->debug('Decoding data with RLE Hybrid', ['bitWidth' => $bitWidth, 'expected_values_count' => $expectedValuesCount, 'reader_position' => ['bits' => $reader->position()->bits(), 'bytes' => $reader->position()->bytes()]]);
    }

    private function readPlainValues(PhysicalType $physicalType, BinaryBufferReader $reader, int $total, ?LogicalType $logicalType, ?int $typeLength) : array
    {
        /** @psalm-suppress PossiblyNullArgument */
        return match ($physicalType) {
            PhysicalType::INT32 => $reader->readInts32($total),
            PhysicalType::INT64 => $reader->readInts64($total),
            PhysicalType::INT96 => $reader->readInts96($total),
            PhysicalType::FLOAT => $reader->readFloats($total),
            PhysicalType::DOUBLE => $reader->readDoubles($total),
            PhysicalType::BYTE_ARRAY => match ($logicalType?->name()) {
                LogicalType::STRING => $reader->readStrings($total),
                default => match ($this->options->get(Option::BYTE_ARRAY_TO_STRING)) {
                    true => $reader->readStrings($total),
                    false => $reader->readByteArrays($total)
                }
            },
            PhysicalType::FIXED_LEN_BYTE_ARRAY => match ($logicalType?->name()) {
                /** @phpstan-ignore-next-line */
                LogicalType::DECIMAL => $reader->readDecimals($total, $typeLength),
                default => throw new RuntimeException('Unsupported logical type ' . ($logicalType?->name() ?: 'null') . ' for FIXED_LEN_BYTE_ARRAY'),
            },
            PhysicalType::BOOLEAN => $reader->readBooleans($total),
        };
    }

    private function readRLEBitPackedHybrid(BinaryBufferReader $reader, RLEBitPackedHybrid $RLEBitPackedHybrid, int $bitWidth, int $expectedValuesCount) : array
    {
        $this->debugLogRLEBitPackedHybridPre($bitWidth, $expectedValuesCount, $reader);
        $data = ($RLEBitPackedHybrid)->decodeHybrid($reader, $bitWidth, $expectedValuesCount);
        $this->debugLogRLEBitPackedHybridPost($data);

        return $data;
    }
}
