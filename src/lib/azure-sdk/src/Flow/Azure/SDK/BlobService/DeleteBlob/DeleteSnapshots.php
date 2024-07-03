<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\DeleteBlob;

enum DeleteSnapshots : string
{
    case INCLUDE = 'include';
    case ONLY = 'only';
}
