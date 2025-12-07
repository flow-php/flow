<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Merge;

use Flow\PgQuery\Protobuf\AST\CmdType;

enum MergeActionType : int
{
    case DELETE = CmdType::CMD_DELETE;
    case DO_NOTHING = CmdType::CMD_NOTHING;
    case INSERT = CmdType::CMD_INSERT;
    case UPDATE = CmdType::CMD_UPDATE;
}
