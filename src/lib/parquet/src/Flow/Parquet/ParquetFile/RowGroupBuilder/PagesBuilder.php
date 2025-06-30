<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\{Option, Options, ParquetFile\RowGroupBuilder\ColumnData\WriteFlatColumnValues};
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\{DataPageBuilder, DictionaryPageBuilder};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, PhysicalType};

final readonly class PagesBuilder
{
    public function __construct(
        private Compressions $compression,
        private PageSizeCalculator $pageSizeCalculator,
        private Options $options,
    ) {
    }

    public function build(FlatColumn $column, WriteFlatColumnValues $data, ColumnChunkStatistics $statistics) : PageContainers
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

        /**
         * Ok, we need to now figure out a way to calculate how many rows can fit into a single page.
         * The edge case here is that when dealing with deeply nested structures, we need to split them
         * equally, otherwise we might end up with different number of rows in different pages, which is not allowed.
         */
        // $rowsPerPage = $this->pageSizeCalculator->rowsPerPage($column, $statistics);
        $rowsPerPage = 100;

        foreach ($data->splitByRows($rowsPerPage) as $rowsChunk) {
            $containers->add((new DataPageBuilder($this->compression, $this->options))->build($column, $rowsChunk));
        }

        return $containers;
    }
}
