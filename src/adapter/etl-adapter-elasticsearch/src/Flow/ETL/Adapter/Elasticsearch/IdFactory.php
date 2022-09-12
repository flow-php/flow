<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\Serializer\Serializable;

/**
 * @template TValue
 *
 * @extends Serializable<TValue>
 *
 * @psalm-immutable
 */
interface IdFactory extends Serializable
{
    public function create(Row $row) : Entry;
}
