<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ETL\Row\Entry;
use Flow\Serializer\Serializable;

/**
 * @template T
 * @extends Serializable<T>
 */
interface Constraint extends Serializable
{
    /**
     * @param Entry $entry
     *
     * @return bool
     */
    public function isSatisfiedBy(Entry $entry) : bool;
}
