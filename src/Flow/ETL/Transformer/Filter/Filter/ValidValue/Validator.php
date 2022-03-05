<?php declare(strict_types=1);

namespace Flow\ETL\Transformer\Filter\Filter\ValidValue;

use Flow\Serializer\Serializable;

/**
 * @template T
 * @extends Serializable<T>
 */
interface Validator extends Serializable
{
    /**
     * @param mixed $value
     *
     * @return bool
     */
    public function isValid($value) : bool;
}
