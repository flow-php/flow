<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ArrayUnpack implements ScalarFunction, ScalarFunction\UnpackResults
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly array $skipKeys = [],
        private readonly ?string $entryPrefix = null
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $array = $this->ref->eval($row);

        if (!\is_array($array)) {
            return null;
        }

        $values = [];

        /**
         * @var int|string $key
         * @var mixed $value
         */
        foreach ($array as $key => $value) {
            $entryName = (string) $key;

            if (\in_array($entryName, $this->skipKeys, true)) {
                continue;
            }

            if ($this->entryPrefix) {
                $entryName = $this->entryPrefix . $entryName;
            }

            $values[$entryName] = $value;
        }

        return $values;
    }

    public function unpack() : bool
    {
        return true;
    }
}
