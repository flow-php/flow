<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\Serializer\Serializable;

/**
 * @template T
 *
 * @extends Serializable<T>
 */
interface DataFrameFactory extends Serializable
{
    public function from(Rows $rows) : DataFrame;
}
