<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;

final class Between implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $lowerBoundRef;
    private readonly ScalarFunction $upperBoundRef;
    private readonly ScalarFunction $boundary;

    public function __construct(
        mixed $value,
        mixed $lowerBoundRef,
        mixed $upperBoundRef,
        ScalarFunction|Boundary $boundary = Boundary::LEFT_INCLUSIVE,
    ) {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->lowerBoundRef = $lowerBoundRef instanceof ScalarFunction ? $lowerBoundRef : lit($lowerBoundRef);
        $this->upperBoundRef = $upperBoundRef instanceof ScalarFunction ? $upperBoundRef : lit($upperBoundRef);
        $this->boundary = $boundary instanceof ScalarFunction ? $boundary : lit($boundary);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->lowerBoundRef, $this->upperBoundRef, $this->boundary];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2], $children[3]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return (new Nullability())->any(
            type_boolean(),
            $this->value->returns(),
            $this->lowerBoundRef->returns(),
            $this->upperBoundRef->returns(),
            $this->boundary->returns(),
        );
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $boundary = (new Parameter($this->boundary))->asEnum($row, $context, Boundary::class);

        if (!$boundary instanceof Boundary) {
            throw new InvalidArgumentException('Between function requires valid boundary');
        }

        return $boundary->compare(
            (new Parameter($this->value))->eval($row, $context),
            (new Parameter($this->lowerBoundRef))->eval($row, $context),
            (new Parameter($this->upperBoundRef))->eval($row, $context),
        );
    }
}
