<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\InvalidArgumentException;

use function strlen;

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
        if ($separator !== null && strlen($separator) !== 1) {
            throw new InvalidArgumentException('Separator must be a single character');
        }

        if ($enclosure !== null && strlen($enclosure) !== 1) {
            throw new InvalidArgumentException('Enclosure must be a single character');
        }

        if ($escape !== null && strlen($escape) > 1) {
            throw new InvalidArgumentException('Escape must be empty or a single character');
        }
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
