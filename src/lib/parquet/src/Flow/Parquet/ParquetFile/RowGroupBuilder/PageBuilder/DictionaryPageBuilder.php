<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use function Flow\Parquet\array_flatten;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DictionaryPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\Type;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageContainer;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TMemoryBuffer;

final class DictionaryPageBuilder implements PageBuilder
{
    public function build(FlatColumn $column, DataConverter $dataConverter, array $rows) : PageContainer
    {
        $dictionary = [];

        foreach (array_flatten($rows) as $value) {
            if (!\in_array($value, $dictionary, true)) {
                $dictionary[] = $value;
            }
        }

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);

        $dictionaryBuffer = '';
        $this->writeData($column, $dictionaryBuffer, $dictionary, $dataConverter);
        $pageWriter->append($dictionaryBuffer);

        $pageHeader = new PageHeader(
            Type::DICTIONARY_PAGE,
            \strlen($pageBuffer),
            \strlen($pageBuffer),
            dataPageHeader: null,
            dataPageHeaderV2: null,
            dictionaryPageHeader: new DictionaryPageHeader(
                Encodings::PLAIN,
                \count($dictionary)
            ),
        );
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $pageBuffer,
            $dictionary,
            $pageHeader
        );
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     */
    private function writeData(FlatColumn $column, string &$valuesBuffer, array $rawValues, DataConverter $dataConverter) : string
    {
        $values = [];

        foreach ($rawValues as $value) {
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
                    case null;
                    (new BinaryBufferWriter($valuesBuffer))->writeInts32($values);

                    break;
                }

                break;
            case PhysicalType::INT64:
                switch ($column->logicalType()?->name()) {
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

        return $valuesBuffer;
    }
}
