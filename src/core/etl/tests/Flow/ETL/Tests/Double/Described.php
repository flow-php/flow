<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use SensitiveParameter;

use function Flow\Types\DSL\type_string;

final readonly class Described implements Describable
{
    public function __construct(
        public string $value,
        public ?Description $nested = null,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(type_string()->assert($data['value'] ?? null));
    }

    public function describe(): Description
    {
        return new Description(
            self::class,
            $this->nested === null ? ['value' => $this->value] : ['value' => $this->value, 'nested' => $this->nested],
        );
    }
}
