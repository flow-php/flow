<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\StyleConverter;

final class StringStyles
{
    public const ADA = 'ada';

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

    public const CAMEL = 'camel';

    public const COBOL = 'cobol';

    public const DOT = 'dot';

    public const KEBAB = 'kebab';

    public const LOWER = 'lower';

    public const MACRO = 'macro';

    public const PASCAL = 'pascal';

    public const SENTENCE = 'sentence';

    public const SNAKE = 'snake';

    public const TITLE = 'title';

    public const TRAIN = 'train';

    public const UPPER = 'upper';

    private function __construct()
    {
    }
}
