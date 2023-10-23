<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Dremel\DataShredded;
use Flow\Dremel\Dremel;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
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
                (new BinaryBufferWriter($valuesBuffer))->writeInts32($values);

                break;
            case PhysicalType::INT64:
                (new BinaryBufferWriter($valuesBuffer))->writeInts64($values);

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
