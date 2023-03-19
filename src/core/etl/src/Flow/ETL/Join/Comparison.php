<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
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
     * @return array<EntryReference>
     */
    public function left() : array;

    /**
     * @return array<EntryReference>
     */
    public function right() : array;
}
