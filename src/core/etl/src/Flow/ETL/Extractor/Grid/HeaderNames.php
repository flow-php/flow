<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\Grid;

use function is_scalar;
use function str_pad;
use function trim;

use const STR_PAD_LEFT;

final readonly class HeaderNames
{
    /**
     * Column names from header cells: a scalar cell as its trimmed text, any other or blank cell as `e` + its position.
     *
     * @param list<mixed> $cells
     *
     * @return list<string>
     */
    public function of(array $cells): array
    {
        $names = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($cells as $index => $cell) {
            $name = trim(is_scalar($cell) ? (string) $cell : '');
            $names[] = $name !== '' ? $name : 'e' . str_pad((string) $index, 2, '0', STR_PAD_LEFT);
        }

        return $names;
    }
}
