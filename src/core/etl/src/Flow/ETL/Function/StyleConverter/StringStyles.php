<?php

declare(strict_types=1);

namespace Flow\ETL\Function\StyleConverter;

use function Symfony\Component\String\u;
use Flow\ETL\Exception\InvalidArgumentException;

enum StringStyles : string
{
    case CAMEL = 'camel';

    case KEBAB = 'kebab';

    case LOWER = 'lower';

    case SNAKE = 'snake';

    case TITLE = 'title';

    case UPPER = 'upper';

    /**
     * @return string[]
     */
    public static function all() : array
    {
        $cases = [];

        foreach (self::cases() as $case) {
            $cases[] = $case->value;
        }

        return $cases;
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function fromString(string $style) : self
    {
        foreach (self::cases() as $case) {
            if ($style === $case->value) {
                return $case;
            }
        }

        throw new InvalidArgumentException("Unrecognized style {$style}, please use one of following: " . \implode(', ', self::all()));
    }

    public function convert(string $value) : string
    {
        return match ($this) {
            self::CAMEL => u($value)->camel()->toString(),
            self::KEBAB => u($value)->kebab()->toString(),
            self::LOWER => u($value)->lower()->toString(),
            self::SNAKE => u($value)->snake()->toString(),
            self::TITLE => u($value)->title()->toString(),
            self::UPPER => u($value)->upper()->toString(),
        };
    }
}
