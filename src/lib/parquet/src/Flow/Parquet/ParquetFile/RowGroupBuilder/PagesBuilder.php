<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\{DataPageBuilder, DictionaryPageBuilder};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, PhysicalType};
use Flow\Parquet\{Option, Options, ParquetFile\RowGroupBuilder\ColumnData\FlatColumnValues};

final class PagesBuilder
{
    public function __construct(
        private readonly Compressions $compression,
        private readonly PageSizeCalculator $pageSizeCalculator,
        private readonly Options $options,
    ) {
    }

    public function build(FlatColumn $column, FlatColumnValues $data, ColumnChunkStatistics $statistics) : PageContainers
    {
        $containers = new PageContainers();

        if ($column->type() !== PhysicalType::BOOLEAN) {
            if ($statistics->cardinalityRation() <= $this->options->get(Option::DICTIONARY_PAGE_MIN_CARDINALITY_RATION)) {
                $dictionaryPageContainer = (new DictionaryPageBuilder($this->compression, $this->options))->build($column, $data);

                if ($dictionaryPageContainer->dataSize() <= $this->options->get(Option::DICTIONARY_PAGE_SIZE)) {
                    $containers->add($dictionaryPageContainer);

                    $containers->add(
                        (new DataPageBuilder($this->compression, $this->options))
                            ->build($column, $data, $dictionaryPageContainer->dictionary, $dictionaryPageContainer->values)
                    );

                    return $containers;
                }
                $dictionaryPageContainer = null;
            }
        }

        foreach ($data->splitByRows($this->pageSizeCalculator->rowsPerPage($column, $statistics)) as $rowsChunk) {
            $containers->add((new DataPageBuilder($this->compression, $this->options))->build($column, $rowsChunk));
        }

        return $containers;
    }
}
