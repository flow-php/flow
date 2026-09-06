<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Mother;

use Google\Service\Sheets\BatchGetValuesResponse;
use Google\Service\Sheets\ValueRange;

use function array_map;

final class SheetValuesMother
{
    /**
     * @param list<null|list<list<mixed>>> $pages null = a page the API returned without `values`
     */
    public static function batch(array $pages): BatchGetValuesResponse
    {
        $response = new BatchGetValuesResponse();
        $response->setValueRanges(array_map(self::range(...), $pages));

        return $response;
    }

    /**
     * @param null|list<list<mixed>> $rows null = the API omitted `values`, so getValues() answers null
     */
    public static function range(?array $rows): ValueRange
    {
        $range = new ValueRange();

        if ($rows !== null) {
            $range->setValues($rows);
        }

        return $range;
    }
}
