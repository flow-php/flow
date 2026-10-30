<?php

declare(strict_types=1);

namespace Flow\ETL\API;

interface Transformation
{
    public function transform(DataFrame $dataFrame): DataFrame;
}
