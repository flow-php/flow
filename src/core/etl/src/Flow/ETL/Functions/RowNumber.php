<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function array_fill;
use function array_keys;
use function Flow\Types\DSL\type_integer;

final class RowNumber implements WindowFunction
{
    public function __construct(
        private readonly ?Window $window = null,
    ) {}

    public function values(Rows $partition, Backend $backend): array
    {
        $count = $partition->count();

        return $count < 1 ? [] : array_keys(array_fill(1, $count, true));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(($data['window'] ?? null) === null ? null : $rebuild->of($data['window'], Window::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['window' => $this->window]);
    }

    /**
     * @return list<FunctionTree>
     */
    public function children(): array
    {
        return [];
    }

    public function deterministic(): bool
    {
        return true;
    }

    public function resolved(): bool
    {
        return true;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        return $this;
    }

    public function over(Window $window): static
    {
        return new self($window);
    }

    /**
     * NOT NULL - every row of a partition has a row number
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_integer();
    }

    public function toString(): string
    {
        return 'row_number()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
