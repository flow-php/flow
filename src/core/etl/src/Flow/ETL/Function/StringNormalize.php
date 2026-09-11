<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Normalizer;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_string;
use function Symfony\Component\String\u;

final class StringNormalize implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $form;

    public function __construct(ScalarFunction|string $value, ScalarFunction|int $form = Normalizer::NFC)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->form = $form instanceof ScalarFunction ? $form : lit($form);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->form];
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
        return type_string();
    }

    public function eval(Row $row, FlowContext $context): ?string
    {
        $value = (new Parameter($this->value))->asString($row, $context);
        $form = (new Parameter($this->form))->asInt($row, $context, Normalizer::NFC);

        if ($value === null) {
            throw new InvalidArgumentException('StringNormalize function requires non-null value');
        }

        return u($value)->normalize($form)->toString();
    }
}
