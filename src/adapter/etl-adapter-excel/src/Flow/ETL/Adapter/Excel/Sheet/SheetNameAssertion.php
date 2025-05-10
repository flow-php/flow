<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Sheet;

use Flow\ETL\Exception\InvalidArgumentException;

final class SheetNameAssertion
{
    private const SHEET_NAME_REGEX = '/^(?!.{32,})[^\/*?:[\]]+$/';

    public static function assert(string $sheetName) : void
    {
        if (!self::isValid($sheetName)) {
            throw new InvalidArgumentException('Sheet name must be a valid Excel sheet name');
        }
    }

    public static function isValid(string $sheetName) : bool
    {
        return \preg_match(self::SHEET_NAME_REGEX, $sheetName) === 1;
    }
}
