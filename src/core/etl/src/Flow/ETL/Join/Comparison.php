<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Row;
use Flow\Serializer\Serializable;

/**
 * @template T
 *
 * @extends Serializable<T>
 *
 * @psalm-immutable
 */
interface Comparison extends Serializable
{
    public function compare(Row $left, Row $right) : bool;

    /**
     * @return array<string>
     */
    public function left() : array;

    /**
     * @return array<string>
     */
    public function right() : array;
}
