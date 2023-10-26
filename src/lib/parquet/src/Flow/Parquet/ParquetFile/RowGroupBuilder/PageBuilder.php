<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\Schema\FlatColumn;

interface PageBuilder
{
    public function build(FlatColumn $column, array $rows) : PageContainer;
}
