<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use InvalidArgumentException as BaseInvalidArgumentException;
use Symfony\Component\Uid\Ulid as SymfonyUlid;

use function class_exists;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;

if (!class_exists(SymfonyUlid::class)) {
    throw new RuntimeException(
        "\Symfony\Component\Uid\Ulid class not found, please add 'symfony/uid' as a dependency to the project first.",
    );
}

final class Ulid implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * Null means "generate a new ULID", not lit(null).
     */
    private readonly ?ScalarFunction $ref;

    public function __construct(ScalarFunction|string|null $ref = null)
    {
        $this->ref = $ref === null ? null : ($ref instanceof ScalarFunction ? $ref : lit($ref));
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return $this->ref === null ? [] : [$this->ref];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0] ?? null);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_string();
    }

    public function eval(Row $row, FlowContext $context): string
    {
        $param = $this->ref === null ? null : (new Parameter($this->ref))->asString($row, $context);

        if (null !== $param) {
            try {
                // A ULID is a string column - the object form cannot be typed as one (L6).
                return SymfonyUlid::fromString($param)->toBase32();
            } catch (BaseInvalidArgumentException $e) {
                throw new InvalidArgumentException('Ulid requires valid ULID string: ' . $e->getMessage(), 0, $e);
            }
        }

        return (new SymfonyUlid())->toBase32();
    }
}
