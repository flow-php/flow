<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_boolean;
use function Symfony\Component\String\b;

final class IsUtf8 implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $string;

    public function __construct(ScalarFunction|string $string)
    {
        $this->string = $string instanceof ScalarFunction ? $string : lit($string);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->string];
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
        return (new Nullability())->any(type_boolean(), $this->string->returns());
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $strings = (new Parameter($this->string))->asStrings($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($strings as $i => $string) {
                if ($string === null) {
                    $results[] = null;

                    continue;
                }

                $results[] = b($string)->isUtf8();
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
