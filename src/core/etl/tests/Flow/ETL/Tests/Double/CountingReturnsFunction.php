<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\FunctionTree;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\ScalarFunctionChain;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

/**
 * Counts returns() calls so a test can pin that a schema derived from the operand's declaration is
 * built once per eval(), not once per element.
 */
final class CountingReturnsFunction implements ScalarFunction
{
    use ScalarFunctionChain;

    public int $returnsCalls = 0;

    /**
     * @param Type<mixed> $type
     */
    public function __construct(
        private readonly ScalarFunction $value,
        private readonly Type $type,
    ) {}

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value];
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->type);
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        return $this->value->eval($rows, $backend);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $this->returnsCalls++;

        return $this->type;
    }
}
