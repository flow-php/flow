<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ArrayUnpack extends ScalarFunctionChain implements ScalarFunction\UnpackResults
{
    public function __construct(
        private readonly ScalarFunction|array $array,
        private readonly ScalarFunction|array $skipKeys = [],
        private readonly ScalarFunction|string|null $entryPrefix = null,
    ) {
    }

    public function eval(Row $row) : ?array
    {
        $array = (new Parameter($this->array))->asArray($row);
        $skipKeys = (new Parameter($this->skipKeys))->asArray($row);
        $entryPrefix = (new Parameter($this->entryPrefix))->asString($row);

        if ($array === null || $skipKeys === null) {
            return null;
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

            if ($entryName) {
                $entryName = $entryPrefix . $entryName;
            }

            $values[$entryName] = $value;
        }

        return $values;
    }
}
