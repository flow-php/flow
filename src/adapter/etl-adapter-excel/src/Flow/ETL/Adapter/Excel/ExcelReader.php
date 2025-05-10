<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

enum ExcelReader : string
{
    case ODS = 'ods';
    case XLSX = 'xlsx';
}
