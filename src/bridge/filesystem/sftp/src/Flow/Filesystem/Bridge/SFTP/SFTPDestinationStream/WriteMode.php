<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\SFTPDestinationStream;

enum WriteMode
{
    case APPEND;

    case BLANK;
}
