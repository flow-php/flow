<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

use Flow\PostgreSql\Protobuf\AST\CmdType;

enum RuleEvent : int
{
    case DELETE = CmdType::CMD_DELETE;
    case INSERT = CmdType::CMD_INSERT;
    case SELECT = CmdType::CMD_SELECT;
    case UPDATE = CmdType::CMD_UPDATE;
}
