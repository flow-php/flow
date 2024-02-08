<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\ArrayParameterType;
use Flow\ETL\Rows;

interface QueryParameter
{
    public function queryParamName() : string;

    /**
     * @return null|array<array-key, null|bool|float|int|string>|bool|float|int|string
     */
    public function toQueryParam(Rows $rows) : mixed;

    public function type() : int|ArrayParameterType|null;
}
