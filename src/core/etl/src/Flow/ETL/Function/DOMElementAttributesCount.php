<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\HTMlElement;
use DOMElement;
use DOMNode;
use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $types = [
            type_instance_of(DOMElement::class),
        ];

        if (class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
        }

        $domElements = (new Parameter($this->domElement))->asTypes($rows, $context, ...$types);
        $results = [];
        $i = 0;

        try {
            foreach ($domElements as $i => $domElement) {
                if ($domElement === null) {
                    throw new InvalidArgumentException('DOMElementAttributesCount requires non-null DOMElement');
                }

                $results[] = $domElement->attributes->length ?? 0;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
