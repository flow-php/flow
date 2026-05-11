<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\JoinType as ProtobufJoinType;

/**
 * Join types enum.
 */
enum JoinType: string
{
    case CROSS = 'CROSS';
    case FULL = 'FULL';
    case FULL_OUTER = 'FULL OUTER';
    case INNER = 'INNER';
    case LEFT = 'LEFT';
    case LEFT_OUTER = 'LEFT OUTER';
    case RIGHT = 'RIGHT';
    case RIGHT_OUTER = 'RIGHT OUTER';

    public static function fromProtobuf(int $joinType): self
    {
        return match ($joinType) {
            ProtobufJoinType::JOIN_INNER => self::INNER,
            ProtobufJoinType::JOIN_LEFT => self::LEFT,
            ProtobufJoinType::JOIN_RIGHT => self::RIGHT,
            ProtobufJoinType::JOIN_FULL => self::FULL,
            default => self::INNER,
        };
    }

    public function toProtobuf(): int
    {
        return match ($this) {
            self::INNER => ProtobufJoinType::JOIN_INNER,
            self::LEFT, self::LEFT_OUTER => ProtobufJoinType::JOIN_LEFT,
            self::RIGHT, self::RIGHT_OUTER => ProtobufJoinType::JOIN_RIGHT,
            self::FULL, self::FULL_OUTER => ProtobufJoinType::JOIN_FULL,
            self::CROSS => ProtobufJoinType::JOIN_INNER, // Cross join is implemented as inner join with no condition
        };
    }
}
