<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration;

use Flow\ETL\Adapter\Excel\ExcelReadOptions;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Tests\FlowTestCase;

use function array_keys;
use function iterator_to_array;

final class WorkbookReaderTest extends FlowTestCase
{
    public function test_options_reach_the_sheet(): void
    {
        $rows = iterator_to_array(
            ExcelFixtureContext::sheet(
                'nullable_fixture.xlsx',
                options: new ExcelReadOptions(withHeader: false, convertEmptyToNull: false, offset: 4),
            )->rows(),
            false,
        );

        static::assertCount(3, $rows);
        static::assertSame(['e00', 'e01', 'e02'], array_keys($rows[0]->values));
        static::assertSame('', $rows[0]->values['e01']);
    }

    public function test_sheet_is_a_new_object_every_time(): void
    {
        $reader = ExcelFixtureContext::reader();
        $source = ExcelFixtureContext::source('fixture.xlsx');

        static::assertNotSame($reader->sheet($source), $reader->sheet($source));
    }
}
