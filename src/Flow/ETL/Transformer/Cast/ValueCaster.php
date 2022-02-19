<?php declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast;

use Flow\Serializer\Serializable;

/**
 * @psalm-immutable
 */
interface ValueCaster extends Serializable
{
    /**
     * @psalm-pure
     *
     * @param mixed $value
     *
     * @return mixed
     */
    public function cast($value);
}
