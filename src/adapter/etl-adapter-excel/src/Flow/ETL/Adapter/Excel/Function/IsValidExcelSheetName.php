<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Function;

use Flow\ETL\Adapter\Excel\Sheet\SheetNameAssertion;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ResolvesFromChildren;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;

final readonly class IsValidExcelSheetName implements ScalarFunction
{
    use ResolvesFromChildren;

    private ScalarFunction $sheetName;

    public function __construct(ScalarFunction|string $sheetName)
    {
        $this->sheetName = $sheetName instanceof ScalarFunction ? $sheetName : lit($sheetName);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->sheetName];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return (new Nullability())->any(type_boolean(), $this->sheetName->returns());
    }

    public function eval(Row $row, FlowContext $context): ?bool
    {
        $sheetName = (new Parameter($this->sheetName))->asString($row, $context);

        if ($sheetName === null) {
            return null;
        }

        return SheetNameAssertion::isValid($sheetName);
    }
}
