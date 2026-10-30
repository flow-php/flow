<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Join\Condition;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Join\Condition;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Kernels\Comparison\ValueEquality;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

use function Flow\Types\DSL\type_string;
use function is_array;
use function is_string;

final readonly class Identical implements Condition
{
    public function __construct(
        private string|Reference $leftReference,
        private string|Reference $rightReference,
    ) {}

    public function compare(Rows $left, Rows $right): array
    {
        $leftColumn = $left->column($this->left()[0]->base());
        $result = [];

        // SQL semantics - null never equals anything, including null
        foreach ((new ValueEquality())->identical(
            $leftColumn,
            $right->column($this->right()[0]->base()),
        ) as $i => $identical) {
            $result[] = $identical && !$leftColumn->isNull($i);
        }

        return $result;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            is_array($data['left'] ?? null)
                ? $rebuild->of($data['left'], Reference::class)
                : type_string()->assert($data['left'] ?? null),
            is_array($data['right'] ?? null)
                ? $rebuild->of($data['right'], Reference::class)
                : type_string()->assert($data['right'] ?? null),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['left' => $this->leftReference, 'right' => $this->rightReference]);
    }

    /**
     * @return array<Reference>
     */
    public function left(): array
    {
        return [
            is_string($this->leftReference) ? UnresolvedReference::init($this->leftReference) : $this->leftReference,
        ];
    }

    /**
     * @return array<Reference>
     */
    public function right(): array
    {
        return [
            is_string($this->rightReference) ? UnresolvedReference::init($this->rightReference) : $this->rightReference,
        ];
    }
}
