<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\CreateContainer;

enum PublicAccess : string
{
    case BLOB = 'blob';
    case CONTAINER = 'container';
}
