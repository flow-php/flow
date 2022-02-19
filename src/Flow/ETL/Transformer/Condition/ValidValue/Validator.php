<?php declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition\ValidValue;

interface Validator
{
    /**
     * @param mixed $value
     *
     * @return bool
     */
    public function isValid($value) : bool;
}
