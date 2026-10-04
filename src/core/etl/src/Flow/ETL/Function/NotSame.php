<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\ComparableValues;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\ValueComparator;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_boolean;

final class NotSame implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $left;
    private readonly ScalarFunction $right;

    public function __construct(mixed $left, mixed $right)
    {
        $this->left = $left instanceof ScalarFunction ? $left : lit($left);
        $this->right = $right instanceof ScalarFunction ? $right : lit($right);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->left, $this->right];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        (new ValueComparator())->assertComparableTypes($this->left->returns(), $this->right->returns(), '!==');

        return type_boolean();
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $leftColumn = (new Parameter($this->left))->column($rows, $context);
        $rightColumn = (new Parameter($this->right))->column($rows, $context);
        $comparable = new ComparableValues();
        [$lefts, $rights] = type_bare($leftColumn->type())::class === type_bare($rightColumn->type())::class
            ? [$comparable->equality($leftColumn), $comparable->equality($rightColumn)]
            : [$leftColumn->values(), $rightColumn->values()];
        $results = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($lefts as $i => $left) {
                // @mago-ignore analysis:mixed-assignment
                $right = $rights[$i];

                // PHP identity, not SQL equality: null !== null is a defined, useful answer
                $results[] = $left !== $right;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
