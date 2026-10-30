<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Tests\Context\ExecutedPlan;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

use function iterator_to_array;

/**
 * Drains $plan on every batch - on the context of the plan advancing it that is a cycle, on another context it is a
 * side input.
 */
final class PlanDrainingTransformer implements Transformer
{
    /**
     * @var list<Rows>
     */
    public array $drained = [];

    public function __construct(
        private readonly LogicalPlan $plan,
        private readonly FlowContext $context,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        $this->drained = [
            ...$this->drained,
            ...iterator_to_array(ExecutedPlan::of($this->plan, $this->context), false),
        ];

        return $rows;
    }
}
