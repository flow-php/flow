<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\Tests\Context\GoogleSheetFixtureContext;
use Flow\ETL\Adapter\GoogleSheet\Tests\Mother\SheetValuesMother;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function iterator_to_array;

final class GoogleSheetSamplerTest extends FlowTestCase
{
    public function test_samples_yields_one_unstarted_unit(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1'], ['2']]));
        $units = GoogleSheetFixtureContext::sampler($values, 2)->samples(2);

        $unit = $units->current();
        static::assertSame([], $values->getCalls);

        $unit->current();
        static::assertCount(1, $values->getCalls);

        $units->next();
        static::assertFalse($units->valid());
    }

    public function test_samples_unit_holds_the_rows(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1'], ['2']]));

        $rows = iterator_to_array(GoogleSheetFixtureContext::sampler($values, 2)->samples(2)->current(), false);

        static::assertSame(
            [['id' => '1'], ['id' => '2']],
            array_map(static fn($rowValues): array => $rowValues->values, $rows),
        );
        static::assertSame('sheet!A1:B3', $values->getCalls[0][1]);
    }

    public function test_header_carries_the_names_the_sample_resolved(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'note'], ['1', 'x']]));

        static::assertSame(['id', 'note'], GoogleSheetFixtureContext::sampler($values, 2)->header());
    }

    public function test_header_is_known_for_a_sheet_that_yields_no_rows(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id', 'note']]));
        $sampler = GoogleSheetFixtureContext::sampler($values, 2);

        static::assertSame(['id', 'note'], $sampler->header());
        static::assertSame([], iterator_to_array($sampler->samples(2)->current(), false));
    }

    public function test_header_and_samples_share_one_request(): void
    {
        $values = GoogleSheetFixtureContext::values(SheetValuesMother::range([['id'], ['1']]));
        $sampler = GoogleSheetFixtureContext::sampler($values, 2);

        $sampler->header();
        iterator_to_array($sampler->samples(2)->current(), false);
        $sampler->header();

        static::assertCount(1, $values->getCalls);
    }
}
