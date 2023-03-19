<?php declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\Serializer\Serializable;

/**
 * @template T
 *
 * @extends Serializable<T>
 */
interface ValueConverter extends Serializable
{
    public function convert(mixed $value) : mixed;
}
