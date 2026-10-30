<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\FunctionTree;
use Flow\ETL\Functions\Literal;
use Flow\ETL\Functions\Parameter;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;
use SensitiveParameter;

use function Flow\Types\DSL\type_boolean;

final readonly class IsValidExcelSheetName implements ScalarFunction
{
    private ScalarFunction $sheetName;

    public function __construct(ScalarFunction|string $sheetName)
    {
        $this->sheetName = $sheetName instanceof ScalarFunction ? $sheetName : new Literal($sheetName);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['sheet_name'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'sheet_name' => $this->sheetName,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->sheetName];
    }

    public function deterministic(): bool
    {
        foreach ($this->children() as $child) {
            if (!$child->deterministic()) {
                return false;
            }
        }

        return true;
    }

    public function resolved(): bool
    {
        foreach ($this->children() as $child) {
            if (!$child->resolved()) {
                return false;
            }
        }

        return true;
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $results = [];

        foreach ((new Parameter($this->sheetName))->asStrings($rows, $backend) as $sheetName) {
            $results[] = $sheetName === null ? null : SheetNameAssertion::isValid($sheetName);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
