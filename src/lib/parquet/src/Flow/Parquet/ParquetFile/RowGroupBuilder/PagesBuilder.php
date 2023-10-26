<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DataPageBuilder;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DictionaryPageBuilder;
use Flow\Parquet\ParquetFile\Schema\ColumnPrimitiveType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class PagesBuilder
{
    public function __construct(
        private readonly DataConverter $dataConverter,
        private readonly PageSizeCalculator $pageSizeCalculator
    ) {
    }

    public function build(FlatColumn $column, array $rows, ColumnChunkStatistics $statistics) : PageContainers
    {
        $containers = new PageContainers();

        if (ColumnPrimitiveType::isString($column)) {
            $dictionaryPageContainer = (new DictionaryPageBuilder($this->dataConverter))->build($column, $rows);

            $containers->add($dictionaryPageContainer);

            /* @phpstan-ignore-next-line */
            foreach (\array_chunk($rows, $this->pageSizeCalculator->rowsPerPage($column, $statistics)) as $rowsChunk) {
                $containers->add((new DataPageBuilder($this->dataConverter, $dictionaryPageContainer->values))->build($column, $rowsChunk));
            }

            return $containers;
        }

        /* @phpstan-ignore-next-line */
        foreach (\array_chunk($rows, $this->pageSizeCalculator->rowsPerPage($column, $statistics)) as $rowsChunk) {
            $containers->add((new DataPageBuilder($this->dataConverter))->build($column, $rowsChunk));
        }

        return $containers;
    }
}
