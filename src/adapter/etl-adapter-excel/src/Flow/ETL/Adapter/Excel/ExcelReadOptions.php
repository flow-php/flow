<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Adapter\Excel\Sheet\SheetNameAssertion;
use Flow\ETL\Exception\InvalidArgumentException;

final readonly class ExcelReadOptions
{
    public function __construct(
        public bool $withHeader = true,
        public bool $convertEmptyToNull = true,
        public int $offset = 1,
        public ?string $sheetName = null,
        public ?ExcelReader $format = null,
    ) {
        if ($offset < 1) {
            throw new InvalidArgumentException('Offset must be greater or equal to 1');
        }

        if ($sheetName !== null) {
            SheetNameAssertion::assert($sheetName);
        }
    }

    public function withConvertEmptyToNull(bool $convertEmptyToNull): self
    {
        return new self(
            withHeader: $this->withHeader,
            convertEmptyToNull: $convertEmptyToNull,
            offset: $this->offset,
            sheetName: $this->sheetName,
            format: $this->format,
        );
    }

    public function withFormat(ExcelReader $format): self
    {
        return new self(
            withHeader: $this->withHeader,
            convertEmptyToNull: $this->convertEmptyToNull,
            offset: $this->offset,
            sheetName: $this->sheetName,
            format: $format,
        );
    }

    public function withHeader(bool $withHeader): self
    {
        return new self(
            withHeader: $withHeader,
            convertEmptyToNull: $this->convertEmptyToNull,
            offset: $this->offset,
            sheetName: $this->sheetName,
            format: $this->format,
        );
    }

    public function withOffset(int $offset): self
    {
        return new self(
            withHeader: $this->withHeader,
            convertEmptyToNull: $this->convertEmptyToNull,
            offset: $offset,
            sheetName: $this->sheetName,
            format: $this->format,
        );
    }

    public function withSheetName(string $sheetName): self
    {
        return new self(
            withHeader: $this->withHeader,
            convertEmptyToNull: $this->convertEmptyToNull,
            offset: $this->offset,
            sheetName: $sheetName,
            format: $this->format,
        );
    }
}
