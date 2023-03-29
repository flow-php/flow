<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\ETL\Rows;

interface QueryParameter
{
    public function queryParamName() : string;

    public function toQueryParam(Rows $rows) : mixed;

    public function type() : ?int;
}
