<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DataPageBuilder;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DictionaryPageBuilder;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;

final class PagesBuilder
{
    public function __construct(private readonly DataConverter $dataConverter)
    {
    }

    public function build(FlatColumn $column, array $rows) : PageContainers
    {
        $containers = new PageContainers();

        $logicalType = $column->logicalType()?->name();

        $dictionaryTypes = [LogicalType::STRING, LogicalType::UUID, LogicalType::ENUM, LogicalType::JSON];

        if ($logicalType !== null && \in_array($logicalType, $dictionaryTypes, true)) {
            $dictionaryPageContainer = (new DictionaryPageBuilder())->build($column, $this->dataConverter, $rows);

            $containers->add($dictionaryPageContainer);
            $containers->add((new DataPageBuilder($dictionaryPageContainer->values))->build($column, $this->dataConverter, $rows));

            return $containers;
        }

        $containers->add((new DataPageBuilder())->build($column, $this->dataConverter, $rows));

        return $containers;
    }
}
