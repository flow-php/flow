<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\BlockBlob;

enum BlockState : string
{
    case COMMITTED = 'Committed';
    case LATEST = 'Latest';
    case UNCOMMITTED = 'Uncommitted';
}
