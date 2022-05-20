<?php declare(strict_types=1);

namespace Flow\ETL;

interface Transformation
{
    public function transform(DataFrame $dataFrame) : DataFrame;
}
