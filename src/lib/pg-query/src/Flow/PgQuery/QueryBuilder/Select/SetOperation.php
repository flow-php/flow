<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Select;

use Flow\PgQuery\Protobuf\AST\SetOperation as ProtobufSetOperation;

enum SetOperation : string
{
    case EXCEPT = 'EXCEPT';
    case EXCEPT_ALL = 'EXCEPT ALL';
    case INTERSECT = 'INTERSECT';
    case INTERSECT_ALL = 'INTERSECT ALL';
    case UNION = 'UNION';
    case UNION_ALL = 'UNION ALL';

    public static function fromProtobuf(int $op, bool $all) : self
    {
        return match ($op) {
            ProtobufSetOperation::SETOP_UNION => $all ? self::UNION_ALL : self::UNION,
            ProtobufSetOperation::SETOP_INTERSECT => $all ? self::INTERSECT_ALL : self::INTERSECT,
            ProtobufSetOperation::SETOP_EXCEPT => $all ? self::EXCEPT_ALL : self::EXCEPT,
            default => self::UNION,
        };
    }

    public function hasAll() : bool
    {
        return match ($this) {
            self::UNION_ALL, self::INTERSECT_ALL, self::EXCEPT_ALL => true,
            default => false,
        };
    }

    public function toProtobuf() : int
    {
        return match ($this) {
            self::UNION, self::UNION_ALL => ProtobufSetOperation::SETOP_UNION,
            self::INTERSECT, self::INTERSECT_ALL => ProtobufSetOperation::SETOP_INTERSECT,
            self::EXCEPT, self::EXCEPT_ALL => ProtobufSetOperation::SETOP_EXCEPT,
        };
    }
}
