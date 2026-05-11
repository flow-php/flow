<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\LockClauseStrength;

/**
 * Lock strength enum for SELECT FOR UPDATE/SHARE.
 */
enum LockStrength: string
{
    case KEY_SHARE = 'KEY SHARE';
    case NO_KEY_UPDATE = 'NO KEY UPDATE';
    case SHARE = 'SHARE';
    case UPDATE = 'UPDATE';

    public static function fromProtobuf(int $strength): self
    {
        return match ($strength) {
            LockClauseStrength::LCS_FORUPDATE => self::UPDATE,
            LockClauseStrength::LCS_FORNOKEYUPDATE => self::NO_KEY_UPDATE,
            LockClauseStrength::LCS_FORSHARE => self::SHARE,
            LockClauseStrength::LCS_FORKEYSHARE => self::KEY_SHARE,
            default => self::UPDATE,
        };
    }

    public function toProtobuf(): int
    {
        return match ($this) {
            self::UPDATE => LockClauseStrength::LCS_FORUPDATE,
            self::NO_KEY_UPDATE => LockClauseStrength::LCS_FORNOKEYUPDATE,
            self::SHARE => LockClauseStrength::LCS_FORSHARE,
            self::KEY_SHARE => LockClauseStrength::LCS_FORKEYSHARE,
        };
    }
}
