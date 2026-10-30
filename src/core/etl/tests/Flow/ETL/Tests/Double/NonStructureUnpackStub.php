<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\FunctionTree;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\ScalarFunction\UnpackResults;
use Flow\ETL\Functions\ScalarFunctionChain;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\ValueColumn;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function array_fill;
use function Flow\Types\DSL\type_string;

/**
 * An UnpackResults whose returns() breaks the structure contract, so a test can reach the guard for it.
 */
final class NonStructureUnpackStub implements UnpackResults
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    use ScalarFunctionChain;

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        return $this;
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        // @mago-ignore analysis:possibly-invalid-argument
        return new ValueColumn(array_fill(0, $rows->count(), []));
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_string();
    }
}
