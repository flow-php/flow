<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Exception\ValidationException;
use Flow\Parquet\ParquetFile\Schema\Column;

interface Validator
{
    /**
     * @throws ValidationException
     */
    public function validate(Column $column, mixed $data) : void;
}
