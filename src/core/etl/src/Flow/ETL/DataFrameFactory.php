<?php declare(strict_types=1);

namespace Flow\ETL;

interface DataFrameFactory
{
    public function from(Rows $rows) : DataFrame;
}
