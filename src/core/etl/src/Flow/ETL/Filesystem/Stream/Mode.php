<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem\Stream;

enum Mode : string
{
    case APPEND_READ_WRITE = 'a+';

    case APPEND_WRITE = 'a';

    case CREATE_READ_WRITE = 'x+';

    case CREATE_WRITE = 'x';
    case READ = 'r';

    case READ_WRITE = 'r+';

    case WRITE = 'w';

    case WRITE_READ = 'w+';
}
