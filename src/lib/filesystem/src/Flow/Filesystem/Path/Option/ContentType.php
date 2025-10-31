<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path\Option;

enum ContentType
{
    case AVRO;
    case BINARY;
    case CSV;
    case JSON;
    case ORC;
    case PARQUET;
    case TEXT;
    case TSV;
    case XML;
    case ZIP;
}
