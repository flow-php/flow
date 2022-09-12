<?php declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\Serializer\Serializable;

/**
 * @template T
 *
 * @extends Serializable<T>
 *
 * @psalm-immutable
 */
interface ValueConverter extends Serializable
{
    /**
     * @psalm-pure
     */
    public function convert(mixed $value) : mixed;
}
