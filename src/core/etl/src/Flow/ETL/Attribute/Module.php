<?php

declare(strict_types=1);

namespace Flow\ETL\Attribute;

enum Module : string
{
    case AZURE_FILESYSTEM = 'AZURE_FILESYSTEM';
    case AZURE_SDK = 'AZURE_SDK';
    case CHART_JS = 'CHART_JS';
    case CORE = 'CORE';
    case CSV = 'CSV';

    case DEPRECATED = 'DEPRECATED';
    case DOCTRINE = 'DOCTRINE';
    case ELASTIC_SEARCH = 'ELASTIC_SEARCH';
    case EXCEL = 'EXCEL';
    case FILESYSTEM = 'FILESYSTEM';
    case GOOGLE_SHEET = 'GOOGLE_SHEET';
    case JSON = 'JSON';
    case MEILI_SEARCH = 'MEILI_SEARCH';
    case PARQUET = 'PARQUET';
    case S3_FILESYSTEM = 'S3_FILESYSTEM';
    case TEXT = 'TEXT';
    case TYPES = 'TYPES';
    case XML = 'XML';
}
