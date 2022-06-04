<?php

declare(strict_types=1);

namespace Flow\ETL\Stream;

enum Mode: string
{
    case READ = 'r';

    case READ_BINARY = 'rb';

    case READ_WRITE = 'r+';

    case WRITE = 'w';

    case WRITE_BINARY = 'wb';
}
