<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\LockWaitPolicy as ProtobufLockWaitPolicy;

/**
 * Lock wait policy enum.
 */
enum LockWaitPolicy: string
{
    case DEFAULT = 'DEFAULT';
    case NOWAIT = 'NOWAIT';
    case SKIP_LOCKED = 'SKIP LOCKED';

    public static function fromProtobuf(int $policy): self
    {
        return match ($policy) {
            ProtobufLockWaitPolicy::LockWaitError => self::NOWAIT,
            ProtobufLockWaitPolicy::LockWaitSkip => self::SKIP_LOCKED,
            default => self::DEFAULT,
        };
    }

    public function toProtobuf(): int
    {
        return match ($this) {
            self::DEFAULT => ProtobufLockWaitPolicy::LockWaitBlock,
            self::NOWAIT => ProtobufLockWaitPolicy::LockWaitError,
            self::SKIP_LOCKED => ProtobufLockWaitPolicy::LockWaitSkip,
        };
    }
}
