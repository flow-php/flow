<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel\Validator;

use Flow\Parquet\Dremel\Validator;
use Flow\Parquet\ParquetFile\Schema\Column;

final class DisabledValidator implements Validator
{
    #[\Override]
    public function validate(Column $column, mixed $data) : void
    {
    }
}
