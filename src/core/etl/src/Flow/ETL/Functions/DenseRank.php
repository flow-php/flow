<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Kernels\Sort\RowOrder;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use RuntimeException as BaseRuntimeException;
use SensitiveParameter;

use function count;
use function Flow\Types\DSL\type_integer;

final class DenseRank implements WindowFunction
{
    public function __construct(
        private readonly ?Window $window = null,
    ) {}

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

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(($data['window'] ?? null) === null ? null : $rebuild->of($data['window'], Window::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['window' => $this->window]);
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        return $this;
    }

    /**
     * @return list<int>
     */
    public function values(Rows $partition, Backend $backend): array
    {
        $orderBy = $this->window()->order();

        if (count($orderBy) === 0) {
            throw new BaseRuntimeException('Dens Rank window function requires to be ordered by one column');
        }

        $ranks = [];
        $rank = 1;

        foreach ((new RowOrder($orderBy))->ties($partition) as $index => $peer) {
            if ($index > 0 && !$peer) {
                $rank++;
            }

            $ranks[] = $rank;
        }

        return $ranks;
    }

    public function over(Window $window): static
    {
        return new self($window);
    }

    /**
     * NOT NULL - every row of a partition has a dense rank
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_integer();
    }

    public function toString(): string
    {
        return 'dens_rank()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
