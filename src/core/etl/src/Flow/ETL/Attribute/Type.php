<?php

declare(strict_types=1);

namespace Flow\ETL\Attribute;

enum Type : string
{
    case AGGREGATING_FUNCTION = 'aggregating functions';
    case COMPARISON = 'comparisons';
    case DATA_FRAME = 'data frame';
    case ENTRY = 'entries';
    case EXTRACTOR = 'extractors';
    case HELPER = 'helpers';
    case LOADER = 'loaders';
    case SCALAR_FUNCTION = 'scalar functions';
    case SCHEMA = 'schema';
    case TRANSFORMER = 'transformers';
    case TYPE = 'types';
    case WINDOW_FUNCTION = 'window functions';
}
