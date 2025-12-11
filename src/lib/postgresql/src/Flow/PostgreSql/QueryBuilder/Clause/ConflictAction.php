<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\OnConflictAction;

/**
 * ON CONFLICT action enum.
 */
enum ConflictAction : string
{
    case NOTHING = 'NOTHING';
    case UPDATE = 'UPDATE';

    public static function fromProtobuf(int $action) : self
    {
        return match ($action) {
            OnConflictAction::ONCONFLICT_UPDATE => self::UPDATE,
            default => self::NOTHING,
        };
    }

    public function toProtobuf() : int
    {
        return match ($this) {
            self::NOTHING => OnConflictAction::ONCONFLICT_NOTHING,
            self::UPDATE => OnConflictAction::ONCONFLICT_UPDATE,
        };
    }
}
