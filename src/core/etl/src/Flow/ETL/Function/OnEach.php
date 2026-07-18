<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

use function Flow\ETL\DSL\array_to_row;

final class OnEach extends ScalarFunctionChain
{
    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     * @param ScalarFunction $function
     * @param bool|ScalarFunction $preserveKeys
     */
    public function __construct(
        private readonly ScalarFunction|array $array,
        private readonly ScalarFunction $function,
        private readonly ScalarFunction|bool $preserveKeys = true,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $value = (new Parameter($this->array))->asArray($row, $context);
        $preserveKeys = (new Parameter($this->preserveKeys))->asBoolean($row, $context);

        if ($value === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('OnEach requires non-null array'));
        }

        $output = [];

        $hydrator = $context->hydrator();

        // @mago-ignore analysis:mixed-assignment
        foreach ($value as $key => $item) {
            if ($preserveKeys) {
                try {
                    $output[$key] = (new Parameter($this->function))->eval(array_to_row([
                        'element' => $item,
                    ], $hydrator), $context);
                } catch (InvalidArgumentException) {
                    $output[$key] = null;
                }
            } else {
                try {
                    $output[] = (new Parameter($this->function))->eval(array_to_row([
                        'element' => $item,
                    ], $hydrator), $context);
                } catch (InvalidArgumentException) {
                    $output[] = null;
                }
            }
        }

        return $output;
    }
}
