<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

enum FailsWhen
{
    case secondBatch;
    case stop;
}
