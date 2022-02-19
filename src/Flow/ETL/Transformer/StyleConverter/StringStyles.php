<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\StyleConverter;

/**
 * @psalm-immutable
 */
final class StringStyles
{
    public const CAMEL = 'camel';

    public const PASCAL = 'pascal';

    public const SNAKE = 'snake';

    public const ADA = 'ada';

    public const MACRO = 'macro';

    public const KEBAB = 'kebab';

    public const TRAIN = 'train';

    public const COBOL = 'cobol';

    public const LOWER = 'lower';

    public const UPPER = 'upper';

    public const TITLE = 'title';

    public const SENTENCE = 'sentence';

    public const DOT = 'dot';

    public const ALL = [
        self::CAMEL,
        self::PASCAL,
        self::SNAKE,
        self::ADA,
        self::MACRO,
        self::KEBAB,
        self::TRAIN,
        self::COBOL,
        self::LOWER,
        self::UPPER,
        self::TITLE,
        self::SENTENCE,
        self::DOT,
    ];

    private function __construct()
    {
    }
}
