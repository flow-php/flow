<?php declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\Serializer\Serializable;

/**
 * @psalm-immutable
 */
interface ValueConverter extends Serializable
{
    /**
     * @psalm-pure
     *
     * @param mixed $value
     *
     * @return mixed
     */
    public function convert($value);
}
