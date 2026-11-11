<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation;

enum ProgressUnit
{
    case rows;
    case groups;
    case files;
}
