<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Reference;

use function array_unique;
use function array_values;
use function count;

final readonly class DeclaredPivotValues implements PivotValues
{
    /**
     * @var list<int|string>
     */
    private array $values;

    public function __construct(int|string ...$values)
    {
        if ($values === []) {
            throw new InvalidArgumentException('Pivot requires at least one value');
        }

        if (count(array_unique($values)) !== count($values)) {
            throw new InvalidArgumentException('Pivot values must be unique');
        }

        $this->values = array_values($values);
    }

    /**
     * @return list<int|string>
     */
    public function all(): array
    {
        return $this->values;
    }

    public function resolve(DataFrame $source, Reference $pivot): self
    {
        return $this;
    }
}
