<?php

declare(strict_types=1);

namespace Flow\ETL\Transformation;

use Flow\ETL\DataFrame;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformation;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\CallbackRowsTransformer;

use function Flow\ETL\DSL\int_schema;

final readonly class AddRowIndex implements Transformation
{
    public function __construct(
        private string $indexColumn = 'index',
        private StartFrom $startFrom = StartFrom::ZERO,
    ) {}

    public function transform(DataFrame $dataFrame): DataFrame
    {
        $index = $this->startFrom === StartFrom::ZERO ? 0 : 1;

        return $dataFrame->rows(new CallbackRowsTransformer(function (Rows $rows) use (&$index): Rows {
            return $rows->map($rows->schema()->add(int_schema($this->indexColumn)), function (Row $row) use (
                &$index,
            ): Row {
                $row = new Row([...$row->values(), $this->indexColumn => $index]);
                $index++;

                return $row;
            });
        }));
    }
}
