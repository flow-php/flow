<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel;

use Flow\Parquet\Exception\ValidationException;
use Flow\Parquet\ParquetFile\Schema\Column;

interface Validator
{
    /**
     * @param int $row the 0-based index of the row within the writer
     *
     * @throws ValidationException
     */
    public function validate(Column $column, mixed $data, int $row): void;
}
