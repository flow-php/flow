<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

enum ExcelWriter
{
    case ODS;
    case XLSX;
}
