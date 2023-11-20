<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\Validator;

use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator;
use Flow\Parquet\ParquetFile\Schema\Column;

final class DisabledValidator implements Validator
{
    public function validate(Column $column, mixed $data) : void
    {
    }
}
