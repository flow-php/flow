<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition;

use Flow\ETL\Row;
use Flow\ETL\Transformer\Condition\ValidValue\Validator;

final class ValidValue implements RowCondition
{
    private string $entryName;

    private Validator $validator;

    /**
     * @param string $entryName
     */
    public function __construct(string $entryName, Validator $validator)
    {
        $this->entryName = $entryName;
        $this->validator = $validator;
    }

    public function isMetFor(Row $row) : bool
    {
        if (!$row->entries()->has($this->entryName)) {
            return false;
        }

        return $this->validator->isValid($row->valueOf($this->entryName));
    }
}
