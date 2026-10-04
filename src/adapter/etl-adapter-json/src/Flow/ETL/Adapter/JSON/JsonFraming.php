<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

enum JsonFraming
{
    case Array;
    case ArrayLines;
    case Lines;

    public function closing(): string
    {
        return match ($this) {
            self::Lines => "\n",
            self::Array => ']',
            self::ArrayLines => "\n]",
        };
    }

    public function empty(): string
    {
        return match ($this) {
            self::Lines => '',
            self::Array => '[]',
            self::ArrayLines => "[\n\n]",
        };
    }

    public function opening(): string
    {
        return match ($this) {
            self::Lines => '',
            self::Array => '[',
            self::ArrayLines => "[\n",
        };
    }

    public function separator(): string
    {
        return match ($this) {
            self::Lines => "\n",
            self::Array => ',',
            self::ArrayLines => ",\n",
        };
    }
}
