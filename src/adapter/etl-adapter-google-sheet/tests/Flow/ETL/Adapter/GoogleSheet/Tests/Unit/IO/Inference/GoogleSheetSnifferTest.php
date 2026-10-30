<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit\IO\Inference;

use Flow\ETL\Adapter\GoogleSheet\Tests\Context\GoogleSheetFixtureContext;
use Flow\ETL\Adapter\GoogleSheet\Tests\Mother\SheetValuesMother;
use Flow\ETL\Tests\FlowTestCase;

final class GoogleSheetSnifferTest extends FlowTestCase
{
    public function test_the_sheet_is_one_unit_fetched_on_the_first_sniff(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1'], ['2']]));
        $sniffer = GoogleSheetFixtureContext::sniffer($values, 2);

        static::assertSame(1, $sniffer->units());
        static::assertSame([], $values->getCalls);

        $sniffer->sniff(0, 2);
        static::assertCount(1, $values->getCalls);
    }

    public function test_a_sniff_folds_the_rows_of_the_range(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1'], ['2']]));

        static::assertSame(2, GoogleSheetFixtureContext::sniffer($values, 2)->sniff(0, 2)->columns->rows);
        static::assertSame('sheet!A1:B3', $values->getCalls[0][1]);
    }

    public function test_header_carries_the_names_the_sample_resolved(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'note'], ['1', 'x']]));

        $sniffer = GoogleSheetFixtureContext::sniffer($values, 2);

        static::assertSame(['id', 'note'], $sniffer->header());
        static::assertSame(['id', 'note'], $sniffer->names());
    }

    public function test_header_is_known_for_a_sheet_that_yields_no_rows(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'note']]));
        $sniffer = GoogleSheetFixtureContext::sniffer($values, 2);

        static::assertSame(['id', 'note'], $sniffer->header());
        static::assertSame(0, $sniffer->sniff(0, 2)->columns->rows);
    }

    public function test_header_and_sniff_share_one_request(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1']]));
        $sniffer = GoogleSheetFixtureContext::sniffer($values, 2);

        $sniffer->header();
        $sniffer->sniff(0, 2);
        $sniffer->header();

        static::assertCount(1, $values->getCalls);
    }

    public function test_grid_row_count_comes_from_the_sample_request(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1']]));
        $sniffer = GoogleSheetFixtureContext::sniffer($values, 2, rowCount: 500);

        static::assertSame(500, $sniffer->gridRowCount());
        $sniffer->header();
        static::assertCount(1, $values->getCalls);
    }
}
