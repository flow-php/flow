<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Context;

use Flow\ETL\Adapter\GoogleSheet\IO\Extractor\Columns;
use Flow\ETL\Adapter\GoogleSheet\IO\Extractor\GoogleSheetExtractor;
use Flow\ETL\Adapter\GoogleSheet\IO\Extractor\GoogleSheetReader;
use Flow\ETL\Adapter\GoogleSheet\IO\Extractor\GoogleSheetReadOptions;
use Flow\ETL\Adapter\GoogleSheet\IO\Inference\GoogleSheetSniffer;
use Flow\ETL\Adapter\GoogleSheet\Tests\Double\SpySpreadsheetsValuesResource;
use Flow\ETL\Adapter\GoogleSheet\Tests\Mother\SheetsServiceMother;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\SchemaInferrer;
use Flow\ETL\Values\Schema;
use Google\Service\Sheets;
use Google\Service\Sheets\BatchGetValuesResponse;
use Google\Service\Sheets\ValueRange;

use function array_values;
use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet_columns;

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
        return (new SchemaInferrer($inference))->infer(
            new GoogleSheetSniffer($reader, $inference->sampleSize, $options->typer($inference->candidates())),
        )->schema;
    }

    public static function reader(
        Sheets $service,
        GoogleSheetReadOptions $options = new GoogleSheetReadOptions(),
    ): GoogleSheetReader {
        return new GoogleSheetReader($service, 'spread-id', self::columns(), $options);
    }

    /**
     * @param null|int<1, max> $rowBudget
     */
    public static function sniffer(
        SpySpreadsheetsValuesResource $values,
        ?int $rowBudget = 20_480,
        int $rowCount = 100,
        GoogleSheetReadOptions $options = new GoogleSheetReadOptions(),
    ): GoogleSheetSniffer {
        return new GoogleSheetSniffer(
            self::reader(self::service($rowCount, $values), $options),
            $rowBudget,
            $options->typer((new SchemaInference())->candidates()),
        );
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
