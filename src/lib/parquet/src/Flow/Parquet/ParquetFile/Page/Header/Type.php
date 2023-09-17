<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page\Header;

enum Type : int
{
    case DATA_PAGE = 0;
    case DATA_PAGE_V2 = 3;
    case DICTIONARY_PAGE = 2;
    case INDEX_PAGE = 1;

    public function isDataPage() : bool
    {
        return $this->value === self::DATA_PAGE->value;
    }
}
