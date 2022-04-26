<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Worker\Pool;

enum WorkerStatus
{
    case connected;
    case disconnected;
    case new;
}
