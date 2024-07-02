<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\ListBlobs;

enum OptionShowOnly : string
{
    case DELETED = 'deleted';
    case FILES = 'files';
    case FOLDERS = 'folders';
}
