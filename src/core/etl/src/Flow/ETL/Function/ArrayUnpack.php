<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Row;

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
    ) {
    }

    /**
     * @return array<string, mixed>
     */
    public function eval(Row $row) : array
    {
        $array = (new Parameter($this->array))->asArray($row);
        $skipKeys = (new Parameter($this->skipKeys))->asArray($row);
        $entryPrefix = (new Parameter($this->entryPrefix))->asString($row);

        if ($array === null || $skipKeys === null) {
            return [];
        }

        $values = [];

        /**
         * @var int|string $key
         * @var mixed $value
         */
        foreach ($array as $key => $value) {
            $entryName = (string) $key;

            if (\in_array($entryName, $skipKeys, true)) {
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
