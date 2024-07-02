<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\ListBlobs;

enum OptionInclude : string
{
    case COPY = 'copy';
    case DELETED = 'deleted';
    case DELETED_WITH_VERSIONS = 'deletedwithversions';
    case IMMUTABILITY_POLICY = 'immutabilitypolicy';
    case LEGAL_HOLD = 'legalhold';
    case METADATA = 'metadata';
    case PERMISSIONS = 'permissions';
    case SNAPSHOTS = 'snapshots';
    case TAGS = 'tags';
    case UNCOMMITTED_BLOBS = 'uncommittedblobs';
    case VERSIONS = 'versions';
}
