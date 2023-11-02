<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem\Stream;

enum Mode : string
{
    case APPEND = 'a';

    case APPEND_BINARY = 'ab';

    case READ = 'r';

    case READ_BINARY = 'rb';

    case READ_WRITE = 'r+';

    case WRITE = 'w';

    case WRITE_BINARY = 'wb';
}
