<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\CSVDialect;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function max;

final readonly class CSVReadOptions
{
    /**
     * @param null|int<1, max> $charactersReadInLine - bytes read per step from a remote stream (S3, Azure); never splits a line
     */
    public function __construct(
        public bool $withHeader = true,
        public bool $emptyToNull = true,
        public bool $removeBOM = true,
        public ?string $separator = null,
        public ?string $enclosure = null,
        public ?string $escape = null,
        public ?int $charactersReadInLine = null,
    ) {
        CSVDialect::assertParts($separator, $enclosure, $escape);
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        return new self(
            type_boolean()->assert($data['with_header'] ?? null),
            type_boolean()->assert($data['empty_to_null'] ?? null),
            type_boolean()->assert($data['remove_bom'] ?? null),
            type_optional(type_string())->assert($data['separator'] ?? null),
            type_optional(type_string())->assert($data['enclosure'] ?? null),
            type_optional(type_string())->assert($data['escape'] ?? null),
            self::positive(type_optional(type_integer())->assert($data['characters_read_in_line'] ?? null)),
        );
    }

    /**
     * @return null|int<1, max>
     */
    public static function positive(?int $value): ?int
    {
        return $value === null ? null : max(1, $value);
    }

    /**
     * @return array{with_header: bool, empty_to_null: bool, remove_bom: bool, separator: null|string, enclosure: null|string, escape: null|string, characters_read_in_line: null|int}
     */
    public function normalize(): array
    {
        return [
            'with_header' => $this->withHeader,
            'empty_to_null' => $this->emptyToNull,
            'remove_bom' => $this->removeBOM,
            'separator' => $this->separator,
            'enclosure' => $this->enclosure,
            'escape' => $this->escape,
            'characters_read_in_line' => $this->charactersReadInLine,
        ];
    }

    /**
     * @param int<1, max> $charactersReadInLine - bytes read per step from a remote stream (S3, Azure); never splits a line
     */
    public function withCharactersReadInLine(int $charactersReadInLine): self
    {
        return new self(
            withHeader: $this->withHeader,
            emptyToNull: $this->emptyToNull,
            removeBOM: $this->removeBOM,
            separator: $this->separator,
            enclosure: $this->enclosure,
            escape: $this->escape,
            charactersReadInLine: $charactersReadInLine,
        );
    }

    public function withEmptyToNull(bool $emptyToNull): self
    {
        return new self(
            withHeader: $this->withHeader,
            emptyToNull: $emptyToNull,
            removeBOM: $this->removeBOM,
            separator: $this->separator,
            enclosure: $this->enclosure,
            escape: $this->escape,
            charactersReadInLine: $this->charactersReadInLine,
        );
    }

    public function withEnclosure(string $enclosure): self
    {
        return new self(
            withHeader: $this->withHeader,
            emptyToNull: $this->emptyToNull,
            removeBOM: $this->removeBOM,
            separator: $this->separator,
            enclosure: $enclosure,
            escape: $this->escape,
            charactersReadInLine: $this->charactersReadInLine,
        );
    }

    public function withEscape(string $escape): self
    {
        return new self(
            withHeader: $this->withHeader,
            emptyToNull: $this->emptyToNull,
            removeBOM: $this->removeBOM,
            separator: $this->separator,
            enclosure: $this->enclosure,
            escape: $escape,
            charactersReadInLine: $this->charactersReadInLine,
        );
    }

    public function withHeader(bool $withHeader): self
    {
        return new self(
            withHeader: $withHeader,
            emptyToNull: $this->emptyToNull,
            removeBOM: $this->removeBOM,
            separator: $this->separator,
            enclosure: $this->enclosure,
            escape: $this->escape,
            charactersReadInLine: $this->charactersReadInLine,
        );
    }

    public function withRemoveBOM(bool $removeBOM): self
    {
        return new self(
            withHeader: $this->withHeader,
            emptyToNull: $this->emptyToNull,
            removeBOM: $removeBOM,
            separator: $this->separator,
            enclosure: $this->enclosure,
            escape: $this->escape,
            charactersReadInLine: $this->charactersReadInLine,
        );
    }

    public function withSeparator(string $separator): self
    {
        return new self(
            withHeader: $this->withHeader,
            emptyToNull: $this->emptyToNull,
            removeBOM: $this->removeBOM,
            separator: $separator,
            enclosure: $this->enclosure,
            escape: $this->escape,
            charactersReadInLine: $this->charactersReadInLine,
        );
    }
}
