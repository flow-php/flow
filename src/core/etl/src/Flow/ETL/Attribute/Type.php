<?php

declare(strict_types=1);

namespace Flow\ETL\Attribute;

enum Type : string
{
    case AGGREGATING_FUNCTION = 'AGGREGATING_FUNCTION';
    case COMPARISON = 'COMPARISON';
    case DATA_FRAME = 'DATA_FRAME';
    case ENTRY = 'ENTRY';
    case EXTRACTOR = 'EXTRACTOR';
    case HELPER = 'HELPER';
    case LOADER = 'LOADER';
    case SCALAR_FUNCTION = 'SCALAR_FUNCTION';
    case SCHEMA = 'SCHEMA';
    case TRANSFORMER = 'TRANSFORMER';
    case TYPE = 'TYPE';
    case WINDOW_FUNCTION = 'WINDOW_FUNCTION';
}
