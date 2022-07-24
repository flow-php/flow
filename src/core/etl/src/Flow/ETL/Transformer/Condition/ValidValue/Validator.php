<?php declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition\ValidValue;

interface Validator
{
    /**
     * @param mixed $value
     */
    public function isValid(mixed $value) : bool;
}
