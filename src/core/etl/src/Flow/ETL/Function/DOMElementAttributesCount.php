<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\HTMlElement;
use DOMElement;
use DOMNode;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function class_exists;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;

final class DOMElementAttributesCount implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $domElement;

    public function __construct(ScalarFunction|DOMNode|HTMlElement $domElement)
    {
        $this->domElement = $domElement instanceof ScalarFunction ? $domElement : lit($domElement);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->domElement];
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
        return type_integer();
    }

    public function eval(Row $row, FlowContext $context): ?int
    {
        $types = [
            type_instance_of(DOMElement::class),
        ];

        if (class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
        }

        $domElement = (new Parameter($this->domElement))->as($row, $context, ...$types);

        if ($domElement === null) {
            throw new InvalidArgumentException('DOMElementAttributesCount requires non-null DOMElement');
        }

        return $domElement->attributes->length ?? 0;
    }
}
