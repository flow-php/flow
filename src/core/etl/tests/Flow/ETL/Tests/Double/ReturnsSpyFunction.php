<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\FunctionTree;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\ScalarFunctionChain;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function array_fill;
use function Flow\Types\DSL\type_integer;

/**
 * Evaluates to 1 per row and counts how often its result type is derived; with $derivable false the type cannot be
 * derived at all.
 */
final class ReturnsSpyFunction implements ScalarFunction
{
    use ScalarFunctionChain;

    public int $derivations = 0;

    public function __construct(
        public readonly bool $derivable = true,
    ) {}

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [];
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        // @mago-ignore analysis:possibly-invalid-argument
        return (new ResultColumn($backend))->of($this, $rows->isEmpty() ? [] : array_fill(0, $rows->count(), 1));
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $this->derivations++;

        if (!$this->derivable) {
            throw SchemaNotDerivableException::function('counting', 'no declared type');
        }

        return type_integer();
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        return $this;
    }
}
