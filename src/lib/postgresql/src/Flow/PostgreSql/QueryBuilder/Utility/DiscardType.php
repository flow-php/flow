<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\DiscardMode;

enum DiscardType: int
{
    case ALL = DiscardMode::DISCARD_ALL;
    case PLANS = DiscardMode::DISCARD_PLANS;
    case SEQUENCES = DiscardMode::DISCARD_SEQUENCES;
    case TEMP = DiscardMode::DISCARD_TEMP;
}
