<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\GetBlockBlobBlockList;

enum BlockListType : string
{
    case ALL = 'all';
    case COMMITTED = 'committed';
    case UNCOMMITTED = 'uncommitted';
}
