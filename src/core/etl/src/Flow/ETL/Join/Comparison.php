<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\Serializer\Serializable;

/**
 * @template T
 *
 * @extends Serializable<T>
 */
interface Comparison extends Serializable
{
    public function compare(Row $left, Row $right) : bool;

    /**
     * @return array<Reference>
     */
    public function left() : array;

    /**
     * @return array<Reference>
     */
    public function right() : array;
}
