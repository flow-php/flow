<?php

declare(strict_types=1);

namespace Flow\ETL\Function\Trim;

enum Type
{
    case BOTH;

    case LEFT;

    case RIGHT;

    public function value() : \Closure
    {
        return match ($this) {
            self::BOTH => static fn (string $value, string $characters = " \t\n\r\0\x0B") : string => \trim($value, $characters),
            self::LEFT => static fn (string $value, string $characters = " \t\n\r\0\x0B") : string => \ltrim($value, $characters),
            self::RIGHT => static fn (string $value, string $characters = " \t\n\r\0\x0B") : string => \rtrim($value, $characters),
        };
    }
}
