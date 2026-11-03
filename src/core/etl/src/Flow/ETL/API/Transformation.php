<?php

declare(strict_types=1);

namespace Flow\ETL\API;

interface Transformation
{
    public function __invoke(DataFrame $dataFrame): DataFrame;
}
