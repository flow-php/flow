<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use Flow\ETL\Row;
use Flow\ETL\Transformer\Condition\ValidValue\Validator;

final class ValidValue implements RowCondition
{
    public function __construct(
        private readonly string $entryName,
        private readonly Validator $validator
    ) {
    }

    public function isMetFor(Row $row) : bool
    {
        if (!$row->entries()->has($this->entryName)) {
            return false;
        }

        return $this->validator->isValid($row->valueOf($this->entryName));
    }
}
