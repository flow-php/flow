<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor;
use Flow\ETL\Adapter\GoogleSheet\Tests\Context\GoogleSheetFixtureContext;
use Flow\ETL\Adapter\GoogleSheet\Tests\Mother\SheetValuesMother;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

final class GoogleSheetExtractorBatchContractTest extends FlowTestCase
{
    public function test_google_sheet_extractor_honours_the_batch_contract(): void
    {
        self::assertExtractorHonoursBatchContract(
            static fn(): GoogleSheetExtractor => GoogleSheetFixtureContext::extractor(GoogleSheetFixtureContext::service(
                4,
                // the first read is served from the sample, the replay of the same extractor from the batch
                GoogleSheetFixtureContext::valuesAndBatches([SheetValuesMother::range([
                    ['id'],
                    ['1'],
                    ['2'],
                    ['3'],
                ])], [SheetValuesMother::batch([[['id'], ['1'], ['2'], ['3']]])]),
            )),
            RowsMother::sequentialIds(3),
        );
    }
}
