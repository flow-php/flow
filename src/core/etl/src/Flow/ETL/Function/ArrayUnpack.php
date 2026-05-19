<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Row;

use function in_array;

final class ArrayUnpack extends ScalarFunctionChain implements UnpackResults
{
    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     * @param array<array-key, mixed>|ScalarFunction $skipKeys
     * @param null|ScalarFunction|string $entryPrefix
     */
    public function __construct(
        private readonly ScalarFunction|array $array,
        private readonly ScalarFunction|array $skipKeys = [],
        private readonly ScalarFunction|string|null $entryPrefix = null,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function eval(Row $row, FlowContext $context): array
    {
        $array = (new Parameter($this->array))->asArray($row, $context);
        $skipKeys = (new Parameter($this->skipKeys))->asArray($row, $context);
        $entryPrefix = (new Parameter($this->entryPrefix))->asString($row, $context);

        if ($array === null || $skipKeys === null) {
            $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('ArrayUnpack requires non-null array and skipKeys'));

            return [];
        }

        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($array as $key => $value) {
            $entryName = (string) $key;

            if (in_array($entryName, $skipKeys, true)) {
                continue;
            }

            if ($entryPrefix && $entryName) {
                $entryName = $entryPrefix . $entryName;
            }

            $values[$entryName] = $value;
        }

        return $values;
    }
}
