<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeaderV2;
use Flow\Parquet\ParquetFile\Page\Header\DictionaryPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\Type;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\Writer\PageContainer;

use function strlen;

final class PageContainerMother
{
    public static function dataPage(
        int $valuesCount = 10,
        string $pageBuffer = 'pagedata',
        Encodings $encoding = Encodings::PLAIN,
    ): PageContainer {
        return new PageContainer(
            $pageBuffer,
            new PageHeader(
                Type::DATA_PAGE,
                strlen($pageBuffer),
                strlen($pageBuffer),
                dataPageHeader: new DataPageHeader(
                    encoding: $encoding,
                    repetitionLevelEncoding: Encodings::RLE,
                    definitionLevelEncoding: Encodings::RLE,
                    valuesCount: $valuesCount,
                ),
                dataPageHeaderV2: null,
                dictionaryPageHeader: null,
            ),
        );
    }

    public static function dataPageV2(
        int $valuesCount = 10,
        string $pageBuffer = 'pagedata',
        Encodings $encoding = Encodings::PLAIN,
    ): PageContainer {
        return new PageContainer(
            $pageBuffer,
            new PageHeader(
                Type::DATA_PAGE_V2,
                strlen($pageBuffer),
                strlen($pageBuffer),
                dataPageHeader: null,
                dataPageHeaderV2: new DataPageHeaderV2(
                    valuesCount: $valuesCount,
                    nullsCount: 0,
                    rowsCount: $valuesCount,
                    encoding: $encoding,
                    definitionsByteLength: 0,
                    repetitionsByteLength: 0,
                    isCompressed: false,
                    statistics: null,
                ),
                dictionaryPageHeader: null,
            ),
        );
    }

    public static function dictionaryPage(
        int $valuesCount = 5,
        string $pageBuffer = 'dictdata',
        Encodings $encoding = Encodings::RLE_DICTIONARY,
    ): PageContainer {
        return new PageContainer(
            $pageBuffer,
            new PageHeader(
                Type::DICTIONARY_PAGE,
                strlen($pageBuffer),
                strlen($pageBuffer),
                dataPageHeader: null,
                dataPageHeaderV2: null,
                dictionaryPageHeader: new DictionaryPageHeader(encoding: $encoding, valuesCount: $valuesCount),
            ),
        );
    }
}
