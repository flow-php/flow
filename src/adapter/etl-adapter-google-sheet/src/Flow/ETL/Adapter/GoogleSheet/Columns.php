<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Webmozart\Assert\Assert;

final class Columns
{
    public function __construct(
        public readonly string $sheetName,
        public readonly string $startColumn,
        public readonly string $endColumn,
    ) {
        Assert::notEmpty($sheetName);
        Assert::unicodeLetters($startColumn);
        Assert::unicodeLetters($endColumn);
        Assert::greaterThanEq($endColumn, $startColumn);
    }
}
