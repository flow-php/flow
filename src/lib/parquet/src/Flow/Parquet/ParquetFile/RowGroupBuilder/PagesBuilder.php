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

    /**
     * @return array<PageContainer>
     */
    public function build(FlatColumn $column, array $rows) : array
    {
        if ($column->logicalType()?->name() === LogicalType::STRING) {
            $dictionaryPageContainer = (new DictionaryPageBuilder())->build($column, $this->dataConverter, $rows);

            return [
                $dictionaryPageContainer,
                (new DataPageBuilder($dictionaryPageContainer->values))->build($column, $this->dataConverter, $rows),
            ];
        }

        return [(new DataPageBuilder())->build($column, $this->dataConverter, $rows)];
    }
}
