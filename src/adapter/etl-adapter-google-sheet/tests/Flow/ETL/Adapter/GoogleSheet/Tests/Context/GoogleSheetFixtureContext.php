<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Context;

use Flow\ETL\Adapter\GoogleSheet\Columns;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetReader;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetReadOptions;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetSampler;
use Flow\ETL\Adapter\GoogleSheet\Tests\Double\SpySpreadsheetsValuesResource;
use Flow\ETL\Adapter\GoogleSheet\Tests\Mother\SheetsServiceMother;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Google\Service\Sheets;
use Google\Service\Sheets\BatchGetValuesResponse;
use Google\Service\Sheets\ValueRange;

use function array_values;
use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet_columns;

/**
 * Builds every unit-test subject, so no test needs a private helper. In-process doubles only - nothing here
 * touches an HTTP stack; the integration queue is Tests\GoogleSheetsContext.
 */
final class GoogleSheetFixtureContext
{
    public static function columns(string $sheet = 'sheet', string $from = 'A', string $to = 'B'): Columns
    {
        return new Columns($sheet, $from, $to);
    }

    public static function extractor(Sheets $service): GoogleSheetExtractor
    {
        return from_google_sheet_columns($service, 'spread-id', 'sheet', 'A', 'B');
    }

    /**
     * The extractor's own inference expression, so a reader-level test asserts the same fold the extractor runs.
     */
    public static function inferFrom(
        GoogleSheetReader $reader,
        SchemaInference $inference,
        GoogleSheetReadOptions $options = new GoogleSheetReadOptions(),
    ): Schema {
        $sampler = new GoogleSheetSampler($reader, $inference->sampleSize);

        return (new SchemaInferrer($inference, $options->typer($inference->candidates())))->infer(
            $sampler->header(),
            $sampler->samples($inference->sampleSize),
        );
    }

    public static function reader(
        Sheets $service,
        GoogleSheetReadOptions $options = new GoogleSheetReadOptions(),
    ): GoogleSheetReader {
        return new GoogleSheetReader($service, 'spread-id', self::columns(), $options);
    }

    /**
     * @param int<1, max>|-1 $rowBudget
     */
    public static function sampler(
        SpySpreadsheetsValuesResource $values,
        int $rowBudget = 20_480,
        int $rowCount = 100,
        GoogleSheetReadOptions $options = new GoogleSheetReadOptions(),
    ): GoogleSheetSampler {
        return new GoogleSheetSampler(self::reader(self::service($rowCount, $values), $options), $rowBudget);
    }

    public static function service(
        int $rowCount,
        SpySpreadsheetsValuesResource $values,
        string $sheet = 'sheet',
    ): Sheets {
        return SheetsServiceMother::withSheet($sheet, $rowCount, $values);
    }

    public static function serviceWithoutSheets(SpySpreadsheetsValuesResource $values): Sheets
    {
        return SheetsServiceMother::withoutSheets($values);
    }

    public static function values(ValueRange ...$ranges): SpySpreadsheetsValuesResource
    {
        return new SpySpreadsheetsValuesResource(array_values($ranges));
    }

    /**
     * A cold undeclared read pulls from BOTH queues - get() for the sample, batchGet() for the read - so a test
     * that exercises the whole pass has to seed them together.
     *
     * @param list<ValueRange> $ranges
     * @param list<BatchGetValuesResponse> $batches
     */
    public static function valuesAndBatches(array $ranges, array $batches): SpySpreadsheetsValuesResource
    {
        return new SpySpreadsheetsValuesResource($ranges, $batches);
    }
}
