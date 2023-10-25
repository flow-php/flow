<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Dremel\DataShredded;
use Flow\Dremel\Dremel;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Data\BitWidth;
use Flow\Parquet\ParquetFile\Data\RLEBitPackedHybrid;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\Type;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageContainer;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TMemoryBuffer;

final class DataPageBuilder implements PageBuilder
{
    public function __construct(private readonly ?array $dictionary = null)
    {
    }

    public function build(FlatColumn $column, DataConverter $dataConverter, array $rows) : PageContainer
    {
        $shredded = (new Dremel())->shred($rows, $column->maxDefinitionsLevel());

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

        if ($this->dictionary === null) {
            $valuesBuffer = '';
            $this->writeData($column, $valuesBuffer, $shredded, $dataConverter);
            $pageWriter->append($valuesBuffer);
        } else {
            $indices = [];

            foreach ($shredded->values as $value) {
                $index = \array_search($value, $this->dictionary, true);

                if (!\is_int($index)) {
                    throw new RuntimeException('Value "' . $value . '" not found in dictionary');
                }

                $indices[] = $index;
            }

            $valuesBuffer = '';
            $indicesBitWidth = BitWidth::fromArray($indices);
            $indicesWriter = new BinaryBufferWriter($valuesBuffer);
            $indicesWriter->writeVarInts32([$indicesBitWidth]);
            $rleBitPackedHybrid->encodeHybrid($indicesWriter, $indices);

            $pageWriter->append($valuesBuffer);
        }

        $pageHeader = new PageHeader(
            Type::DATA_PAGE,
            \strlen($pageBuffer),
            \strlen($pageBuffer),
            dataPageHeader: new DataPageHeader(
                $this->dictionary ? Encodings::PLAIN_DICTIONARY : Encodings::PLAIN,
                \count($shredded->values),
            ),
            dataPageHeaderV2: null,
            dictionaryPageHeader: null,
        );
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $pageBuffer,
            $shredded->values,
            $pageHeader
        );
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     */
    private function writeData(FlatColumn $column, string &$valuesBuffer, DataShredded $shredded, DataConverter $dataConverter) : void
    {
        $values = [];

        foreach ($shredded->values as $value) {
            $values[] = $dataConverter->toParquetType($column, $value);
        }

        switch ($column->type()) {
            case PhysicalType::BOOLEAN:
                (new BinaryBufferWriter($valuesBuffer))->writeBooleans($values);

                break;
            case PhysicalType::INT32:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::DATE:
                        (new BinaryBufferWriter($valuesBuffer))->writeInts32($values);

                        break;
                    case null:
                        (new BinaryBufferWriter($valuesBuffer))->writeInts32($values);

                        break;
                }

                break;
            case PhysicalType::INT64:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::TIME:
                    case LogicalType::TIMESTAMP:
                        (new BinaryBufferWriter($valuesBuffer))->writeInts64($values);

                        break;
                    case null:
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
    }
}
