<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

enum Materialization
{
    case streaming;
    case blocking;
}
