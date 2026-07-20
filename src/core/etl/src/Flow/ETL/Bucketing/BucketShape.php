<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

/**
 * Column names of the bucket metadata Row yielded by BucketingProcessor, one Row per bucket.
 */
enum BucketShape: string
{
    case id = '_bucket_id';
    case totalRows = '_bucket_total_rows';
}
