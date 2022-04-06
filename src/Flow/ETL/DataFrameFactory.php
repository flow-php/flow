<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\Serializer\Serializable;

/**
 * @template T
 * @extends Serializable<T>
 */
interface DataFrameFactory extends Serializable
{
    /**
     * @param Rows $rows
     *
     * @return DataFrame
     */
    public function from(Rows $rows) : DataFrame;
}
