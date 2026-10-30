<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use InvalidArgumentException as BaseInvalidArgumentException;
use SensitiveParameter;
use Symfony\Component\Uid\Ulid as SymfonyUlid;

use function class_exists;
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
        $this->ref = $ref === null ? null : ($ref instanceof ScalarFunction ? $ref : new Literal($ref));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(($data['ref'] ?? null) === null ? null : $rebuild->of($data['ref'], ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'ref' => $this->ref,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return $this->ref === null ? [] : [$this->ref];
    }

    public function deterministic(): bool
    {
        // ulid() generates, ulid($ref) only converts $ref
        return $this->ref !== null && $this->ref->deterministic();
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $refs = (new Parameter($this->ref))->asStrings($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($refs as $i => $ref) {
                $param = $this->ref === null ? null : $ref;

                if (null !== $param) {
                    try {
                        // A ULID is a string column - the object form cannot be typed as one.
                        $results[] = SymfonyUlid::fromString($param)->toBase32();

                        continue;
                    } catch (BaseInvalidArgumentException $e) {
                        throw new InvalidArgumentException(
                            'Ulid requires valid ULID string: ' . $e->getMessage(),
                            0,
                            $e,
                        );
                    }
                }

                $results[] = (new SymfonyUlid())->toBase32();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
