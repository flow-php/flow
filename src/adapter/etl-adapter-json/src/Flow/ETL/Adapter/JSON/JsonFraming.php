<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

enum JsonFraming
{
    case ARRAY;
    case ARRAY_LINES;
    case LINES;

    public function closing(): string
    {
        return match ($this) {
            self::LINES => "\n",
            self::ARRAY => ']',
            self::ARRAY_LINES => "\n]",
        };
    }

    public function empty(): string
    {
        return match ($this) {
            self::LINES => '',
            self::ARRAY => '[]',
            self::ARRAY_LINES => "[\n\n]",
        };
    }

    public function opening(): string
    {
        return match ($this) {
            self::LINES => '',
            self::ARRAY => '[',
            self::ARRAY_LINES => "[\n",
        };
    }

    public function separator(): string
    {
        return match ($this) {
            self::LINES => "\n",
            self::ARRAY => ',',
            self::ARRAY_LINES => ",\n",
        };
    }
}
