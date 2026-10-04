<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\Grid;

use DateTimeImmutable;
use Flow\ETL\Extractor\Grid\HeaderNames;
use Flow\ETL\Tests\FlowTestCase;

final class HeaderNamesTest extends FlowTestCase
{
    public function test_scalar_cells_are_trimmed_text(): void
    {
        static::assertSame(['id', '1', '2.5', '1'], (new HeaderNames())->of([' id ', 1, 2.5, true]));
    }

    public function test_a_blank_or_non_scalar_cell_is_named_by_its_position(): void
    {
        static::assertSame(
            ['e00', 'e01', 'e02', 'name', 'e04'],
            (new HeaderNames())->of(['', null, '  ', 'name', new DateTimeImmutable('2026-01-01')]),
        );
    }
}
