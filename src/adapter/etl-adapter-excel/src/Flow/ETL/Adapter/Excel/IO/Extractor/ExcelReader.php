<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\IO\Extractor;

enum ExcelReader: string
{
    case ODS = 'ods';

    case XLSX = 'xlsx';
}
