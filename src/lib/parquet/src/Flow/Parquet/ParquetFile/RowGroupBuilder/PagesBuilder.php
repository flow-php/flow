<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DataPageBuilder;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DictionaryPageBuilder;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class PagesBuilder
{
    public function __construct(
        private readonly DataConverter $dataConverter,
        private readonly Compressions $compression,
        private readonly PageSizeCalculator $pageSizeCalculator,
        private readonly Options $options
    ) {
    }

    public function build(FlatColumn $column, array $rows, ColumnChunkStatistics $statistics) : PageContainers
    {
        $containers = new PageContainers();

        if ($column->type() !== PhysicalType::BOOLEAN) {
            if ($statistics->cardinalityRation() <= $this->options->get(Option::DICTIONARY_PAGE_MIN_CARDINALITY_RATION)) {
                $dictionaryPageContainer = (new DictionaryPageBuilder($this->dataConverter, $this->compression, $this->options))->build($column, $rows);

                if ($dictionaryPageContainer->dataSize() <= $this->options->get(Option::DICTIONARY_PAGE_SIZE)) {
                    $containers->add($dictionaryPageContainer);

                    $containers->add(
                        (new DataPageBuilder($this->dataConverter, $this->compression, $this->options))->build($column, $rows, $dictionaryPageContainer->dictionary, $dictionaryPageContainer->values)
                    );

                    return $containers;
                }
                $dictionaryPageContainer = null;
            }
        }

        /* @phpstan-ignore-next-line */
        foreach (\array_chunk($rows, $this->pageSizeCalculator->rowsPerPage($column, $statistics)) as $rowsChunk) {
            $containers->add((new DataPageBuilder($this->dataConverter, $this->compression, $this->options))->build($column, $rowsChunk));
        }

        return $containers;
    }
}
