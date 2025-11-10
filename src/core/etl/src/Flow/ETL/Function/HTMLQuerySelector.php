<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\type_instance_of;
use Dom\{Element, HTMLDocument, HTMLElement};
use Flow\ETL\Exception\{InvalidArgumentException, RequiredPHPVersionException};
use Flow\ETL\{FlowContext, Row};

final class HTMLQuerySelector extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
        private readonly ScalarFunction|string $selector,
    ) {
        if (!\class_exists('\Dom\HTMLDocument')) {
            throw new RequiredPHPVersionException('\Dom\HTMLDocument', '8.4');
        }
    }

    public function eval(Row $row, FlowContext $context) : ?Element
    {
        $value = (new Parameter($this->value))->as($row, $context, type_instance_of(HTMLDocument::class), type_instance_of(HTMLElement::class));
        $selector = (new Parameter($this->selector))->asString($row, $context);

        if (null === $value) {
            return $context->functions()->invalidResult(new InvalidArgumentException('HTMLQuerySelector requires non-null HTMLDocument'));
        }

        if (null === $selector) {
            return $context->functions()->invalidResult(new InvalidArgumentException('HTMLQuerySelector requires non-null selector'));
        }

        return $value->querySelector($selector);
    }
}
