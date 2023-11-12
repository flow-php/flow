<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use Flow\Serializer\Serializable;

/**
 * @template T
 *
 * @extends Serializable<T>
 */
interface Type extends Serializable
{
    public function isEqual(self $type) : bool;

    public function isValid(mixed $value) : bool;

    public function nullable() : bool;

    public function toString() : string;
}
