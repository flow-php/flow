<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Merge;

use Flow\PgQuery\Protobuf\AST\MergeMatchKind as ProtoMergeMatchKind;

enum MergeMatchKind : int
{
    case MATCHED = ProtoMergeMatchKind::MERGE_WHEN_MATCHED;
    case NOT_MATCHED_BY_SOURCE = ProtoMergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE;
    case NOT_MATCHED_BY_TARGET = ProtoMergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_TARGET;
}
