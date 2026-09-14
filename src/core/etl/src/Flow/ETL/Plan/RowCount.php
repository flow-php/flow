<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

enum RowCount
{
    case source;
    case preserving;
    case reducing;
    case expanding;
    case unknown;
}
