<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\ValueCaster;

use Flow\ETL\Transformer\Cast\ValueCaster;

/**
 * @psalm-immutable
 */
final class AnyToArrayCaster implements ValueCaster
{
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    /**
     * @param mixed $value
     *
     * @return array<mixed>
     */
    public function cast($value) : array
    {
        return (array) $value;
    }
}
