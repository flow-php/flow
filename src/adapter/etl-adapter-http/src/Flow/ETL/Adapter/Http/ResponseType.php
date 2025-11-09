<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

enum ResponseType : string
{
    case HTML = 'html';
    case JSON = 'json';
    case TEXT = 'text';
    case XML = 'xml';
}
