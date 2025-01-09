<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page\Header;

use Flow\Parquet\ParquetFile\Encodings;

final readonly class DataPageHeader
{
    public function __construct(
        private Encodings $encoding,
        private Encodings $repetitionLevelEncoding,
        private Encodings $definitionLevelEncoding,
        private int $valuesCount,
    ) {
    }

    public static function fromThrift(\Flow\Parquet\Thrift\DataPageHeader $thrift) : self
    {
        return new self(
            Encodings::from($thrift->encoding),
            Encodings::from($thrift->repetition_level_encoding),
            Encodings::from($thrift->definition_level_encoding),
            $thrift->num_values
        );
    }

    public function definitionLevelEncoding() : Encodings
    {
        return $this->definitionLevelEncoding;
    }

    public function encoding() : Encodings
    {
        return $this->encoding;
    }

    public function repetitionLevelEncoding() : Encodings
    {
        return $this->repetitionLevelEncoding;
    }

    public function toThrift() : \Flow\Parquet\Thrift\DataPageHeader
    {
        return new \Flow\Parquet\Thrift\DataPageHeader([
            'num_values' => $this->valuesCount,
            'encoding' => $this->encoding->value,
            'definition_level_encoding' => $this->definitionLevelEncoding->value,
            'repetition_level_encoding' => $this->repetitionLevelEncoding->value,
        ]);
    }

    public function valuesCount() : int
    {
        return $this->valuesCount;
    }
}
