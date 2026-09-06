<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Schema\Inference\InferredTypes;
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use Flow\Types\Type\TypeNarrower;

/**
 * @type GoogleSheetOptions = array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string}
 */
final readonly class GoogleSheetReadOptions
{
    /**
     * @param GoogleSheetOptions $options
     */
    public function __construct(
        public bool $withHeader = true,
        public bool $dropExtraColumns = true,
        public bool $emptyToNull = true,
        public array $options = [],
    ) {}

    /**
     * A fresh encoder per pass - the sample and the read each consume their own header row.
     */
    public function encoder(): GoogleSheetEncoder
    {
        return new GoogleSheetEncoder($this->withHeader, $this->dropExtraColumns, $this->emptyToNull);
    }

    /**
     * FORMATTED_VALUE is the only render option under which every cell arrives as a string, so it is the only one
     * where the string ladder has anything to read; the others hand over typed JSON scalars.
     */
    public function typer(InferredTypes $candidates): TypeNarrower
    {
        return ($this->options['valueRenderOption'] ?? 'FORMATTED_VALUE') === 'FORMATTED_VALUE'
            ? new StringTypeNarrower($candidates->toArray())
            : new InstanceOfTypeNarrower();
    }

    public function withDropExtraColumns(bool $dropExtraColumns): self
    {
        return new self($this->withHeader, $dropExtraColumns, $this->emptyToNull, $this->options);
    }

    public function withEmptyToNull(bool $emptyToNull): self
    {
        return new self($this->withHeader, $this->dropExtraColumns, $emptyToNull, $this->options);
    }

    public function withHeader(bool $withHeader): self
    {
        return new self($withHeader, $this->dropExtraColumns, $this->emptyToNull, $this->options);
    }

    /**
     * @param GoogleSheetOptions $options
     */
    public function withOptions(array $options): self
    {
        return new self($this->withHeader, $this->dropExtraColumns, $this->emptyToNull, $options);
    }
}
