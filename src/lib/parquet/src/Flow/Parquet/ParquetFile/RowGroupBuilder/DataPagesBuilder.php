<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Dremel\DataShredded;
use Flow\Dremel\Dremel;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Data\RLEBitPackedHybrid;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\Type;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TMemoryBuffer;

final class DataPagesBuilder
{
    public function __construct(private readonly array $rows)
    {
    }

    public function build(FlatColumn $column) : DataPageContainer
    {
        $shredded = (new Dremel())->shred($this->rows, $column->maxDefinitionsLevel());

        $rleBitPackedHybrid = new RLEBitPackedHybrid();

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);

        if ($column->maxRepetitionsLevel() > 0) {
            $repetitionsBuffer = '';
            $rleBitPackedHybrid->encodeHybrid(new BinaryBufferWriter($repetitionsBuffer), $shredded->repetitions);
            $pageWriter->writeInts32([\strlen($repetitionsBuffer)]);
            $pageWriter->append($repetitionsBuffer);
        }

        if ($column->maxDefinitionsLevel() > 0) {
            $definitionsBuffer = '';
            $rleBitPackedHybrid->encodeHybrid(new BinaryBufferWriter($definitionsBuffer), $shredded->definitions);
            $pageWriter->writeInts32([\strlen($definitionsBuffer)]);
            $pageWriter->append($definitionsBuffer);
        }

        $valuesBuffer = '';
        $valuesBuffer = $this->writeData($column, $valuesBuffer, $shredded);
        $pageWriter->append($valuesBuffer);

        $pageHeader = new PageHeader(
            Type::DATA_PAGE,
            \strlen($pageBuffer),
            dataPageHeader: new DataPageHeader(
                Encodings::PLAIN,
                $this->valuesCount($this->rows),
            ),
            dataPageHeaderV2: null,
            dictionaryPageHeader: null,
        );
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new DataPageContainer(
            $pageHeaderBuffer->getBuffer(),
            $pageBuffer,
            $pageHeader
        );
    }

    public function valuesCount(array $rows) : int
    {
        $valuesCount = 0;

        foreach ($rows as $row) {
            if (\is_array($row)) {
                $valuesCount += $this->valuesCount($row);
            } elseif ($row !== null) {
                $valuesCount++;
            }
        }

        return $valuesCount;
    }

    /**
     * @param array<int, \DateTime|\DateTimeImmutable> $dates
     */
    private function convertToDaysSinceUnix(array $dates) : array
    {
        $days = [];

        foreach ($dates as $date) {
            $epoch = new \DateTimeImmutable('1970-01-01 00:00:00 UTC');
            $interval = $epoch->diff($date->setTime(0, 0, 0, 0));
            $days[] = (int) $interval->format('%a');
        }

        return $days;
    }

    /**
     * @psalm-suppress ArgumentTypeCoercion
     *
     * @param array<int, \DateTimeInterface> $dates
     */
    private function convertToTimestamps(array $dates, LogicalType\Timestamp $timeUnit) : array
    {
        $timestamps = [];

        foreach ($dates as $date) {
            if ($timeUnit->millis()) {
                throw new RuntimeException('Milliseconds precision is not supported yet');
            }

            if ($timeUnit->micros()) {
                $microseconds = \bcadd(\bcmul($date->format('U'), '1000000'), $date->format('u'));
                $timestamps[] = (int) $microseconds;
            } elseif ($timeUnit->nanos()) {
                throw new RuntimeException('Nanoseconds precision is not supported in PHP');
            }
        }

        return $timestamps;
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     */
    private function writeData(FlatColumn $column, string $valuesBuffer, DataShredded $shredded) : string
    {
        $values = $shredded->values;

        switch ($column->type()) {
            case PhysicalType::BOOLEAN:
                (new BinaryBufferWriter($valuesBuffer))->writeBooleans($values);

                break;
            case PhysicalType::INT32:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::DATE:
                        (new BinaryBufferWriter($valuesBuffer))->writeInts32($this->convertToDaysSinceUnix($values));

                        break;
                    case null;
                    (new BinaryBufferWriter($valuesBuffer))->writeInts32($values);

                    break;
                }

                break;
            case PhysicalType::INT64:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::TIMESTAMP:
                        /** @phpstan-ignore-next-line */
                        (new BinaryBufferWriter($valuesBuffer))->writeInts64($this->convertToTimestamps($values, $column->logicalType()?->timestampData()));

                        break;
                    case null;
                    (new BinaryBufferWriter($valuesBuffer))->writeInts64($values);

                    break;
                }

                break;
            case PhysicalType::FLOAT:
                (new BinaryBufferWriter($valuesBuffer))->writeFloats($values);

                break;
            case PhysicalType::DOUBLE:
                (new BinaryBufferWriter($valuesBuffer))->writeDoubles($values);

                break;
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                switch($column->logicalType()?->name()) {
                    case LogicalType::DECIMAL:
                        /** @phpstan-ignore-next-line */
                        (new BinaryBufferWriter($valuesBuffer))->writeDecimals($values, $column->typeLength(), $column->precision(), $column->scale());

                        break;

                    default:
                        throw new \RuntimeException('Writing logical type "' . ($column->logicalType()?->name() ?: 'UNKNOWN') . '" is not implemented yet');
                }

                break;
            case PhysicalType::BYTE_ARRAY:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::JSON:
                    case LogicalType::UUID:
                    case LogicalType::STRING:
                        (new BinaryBufferWriter($valuesBuffer))->writeStrings($values);

                        break;

                    default:
                        throw new \RuntimeException('Writing logical type "' . ($column->logicalType()?->name() ?: 'UNKNOWN') . '" is not implemented yet');
                }

                break;

            default:
                throw new \RuntimeException('Writing physical type "' . $column->type()->name . '" is not implemented yet');
        }

        return $valuesBuffer;
    }
}
