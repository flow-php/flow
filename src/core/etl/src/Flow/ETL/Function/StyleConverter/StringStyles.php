<?php

declare(strict_types=1);

namespace Flow\ETL\Function\StyleConverter;

use Flow\ETL\Exception\InvalidArgumentException;

enum StringStyles : string
{
    case ADA = 'ada';

    case CAMEL = 'camel';

    case COBOL = 'cobol';

    case DOT = 'dot';

    case KEBAB = 'kebab';

    case LOWER = 'lower';

    case MACRO = 'macro';

    case PASCAL = 'pascal';

    case SENTENCE = 'sentence';

    case SNAKE = 'snake';

    case TITLE = 'title';

    case TRAIN = 'train';

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
}
