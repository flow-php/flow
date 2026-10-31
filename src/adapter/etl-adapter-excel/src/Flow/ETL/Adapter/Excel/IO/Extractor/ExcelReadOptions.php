<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\IO\Extractor;

use Flow\ETL\Adapter\Excel\Functions\SheetNameAssertion;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

final readonly class ExcelReadOptions
{
    public function __construct(
        public bool $withHeader = true,
        public bool $convertEmptyToNull = true,
        public ?string $sheetName = null,
        public ?ExcelReader $format = null,
        public bool $missingColumnsAsNull = true,
    ) {
        if ($sheetName !== null) {
            SheetNameAssertion::assert($sheetName);
        }
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        $format = type_optional(type_string())->assert($data['format'] ?? null);

        return new self(
            type_boolean()->assert($data['with_header'] ?? null),
            type_boolean()->assert($data['convert_empty_to_null'] ?? null),
            type_optional(type_string())->assert($data['sheet_name'] ?? null),
            $format === null ? null : ExcelReader::from($format),
            type_boolean()->assert($data['missing_columns_as_null'] ?? null),
        );
    }

    /**
     * @return array{with_header: bool, convert_empty_to_null: bool, sheet_name: null|string, format: null|string, missing_columns_as_null: bool}
     */
    public function normalize(): array
    {
        return [
            'with_header' => $this->withHeader,
            'convert_empty_to_null' => $this->convertEmptyToNull,
            'sheet_name' => $this->sheetName,
            'format' => $this->format?->value,
            'missing_columns_as_null' => $this->missingColumnsAsNull,
        ];
    }

    public function withConvertEmptyToNull(bool $convertEmptyToNull): self
    {
        return new self(
            withHeader: $this->withHeader,
            convertEmptyToNull: $convertEmptyToNull,
            sheetName: $this->sheetName,
            format: $this->format,
            missingColumnsAsNull: $this->missingColumnsAsNull,
        );
    }

    public function withFormat(ExcelReader $format): self
    {
        return new self(
            withHeader: $this->withHeader,
            convertEmptyToNull: $this->convertEmptyToNull,
            sheetName: $this->sheetName,
            format: $format,
            missingColumnsAsNull: $this->missingColumnsAsNull,
        );
    }

    public function withHeader(bool $withHeader): self
    {
        return new self(
            withHeader: $withHeader,
            convertEmptyToNull: $this->convertEmptyToNull,
            sheetName: $this->sheetName,
            format: $this->format,
            missingColumnsAsNull: $this->missingColumnsAsNull,
        );
    }

    public function withMissingColumnsAsNull(bool $missingColumnsAsNull): self
    {
        return new self(
            withHeader: $this->withHeader,
            convertEmptyToNull: $this->convertEmptyToNull,
            sheetName: $this->sheetName,
            format: $this->format,
            missingColumnsAsNull: $missingColumnsAsNull,
        );
    }

    public function withSheetName(string $sheetName): self
    {
        return new self(
            withHeader: $this->withHeader,
            convertEmptyToNull: $this->convertEmptyToNull,
            sheetName: $sheetName,
            format: $this->format,
            missingColumnsAsNull: $this->missingColumnsAsNull,
        );
    }
}
