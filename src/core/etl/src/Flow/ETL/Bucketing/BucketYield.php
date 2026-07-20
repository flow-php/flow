<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

enum BucketYield
{
    case none;
    case rows;
}
