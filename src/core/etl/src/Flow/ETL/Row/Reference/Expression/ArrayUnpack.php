<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ArrayUnpack implements Expression, Row\Reference\UnpackResults
{
    public function __construct(
        private readonly Expression $ref,
        private readonly array $skipKeys = [],
        private readonly ?string $entryPrefix = null
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $array = $this->ref->eval($row);

        if (!\is_array($array)) {
            throw new RuntimeException(\get_class($this->ref) . ' is not an array entry');
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

            /** @psalm-suppress MixedAssignment */
            $values[$entryName] = $value;
        }

        return $values;
    }

    public function unpack() : bool
    {
        return true;
    }
}
