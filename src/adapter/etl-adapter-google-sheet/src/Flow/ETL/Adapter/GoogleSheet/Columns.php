<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Exception\InvalidArgumentException;

final class Columns
{
    public function __construct(
        public readonly string $sheetName,
        public readonly string $startColumn,
        public readonly string $endColumn,
    ) {
        if ('' === $sheetName) {
            throw new InvalidArgumentException('Sheet name can\'t be empty');
        }

        if (!\preg_match('/^[A-Z]+$/u', $startColumn)) {
            throw new InvalidArgumentException(\sprintf('The column `%s` needs to contain only letters.', $startColumn));
        }

        if (!\preg_match('/^[A-Z]+$/u', $endColumn)) {
            throw new InvalidArgumentException(\sprintf('The column `%s` needs to contain only letters.', $endColumn));
        }

        if ($endColumn < $startColumn) {
            throw new InvalidArgumentException(\sprintf(
                'The column that starts the range `%s` must not be after the end column `%s`',
                $startColumn,
                $endColumn
            ));
        }
    }
}
