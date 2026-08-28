<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function in_array;

final class ArrayUnpack implements ScalarFunction, UnpackResults
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $array;
    private readonly ScalarFunction $skipKeys;
    private readonly ScalarFunction $entryPrefix;

    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     * @param array<array-key, mixed>|ScalarFunction $skipKeys
     */
    public function __construct(
        ScalarFunction|array $array,
        ScalarFunction|array $skipKeys = [],
        ScalarFunction|string|null $entryPrefix = null,
    ) {
        $this->array = $array instanceof ScalarFunction ? $array : lit($array);
        $this->skipKeys = $skipKeys instanceof ScalarFunction ? $skipKeys : lit($skipKeys);
        $this->entryPrefix = $entryPrefix instanceof ScalarFunction ? $entryPrefix : lit($entryPrefix);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->array, $this->skipKeys, $this->entryPrefix];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        throw SchemaNotDerivableException::function(
            'array_unpack',
            'it produces N columns whose names come from runtime values',
        );
    }

    /**
     * @return array<string, mixed>
     */
    public function eval(Row $row, FlowContext $context): array
    {
        $array = (new Parameter($this->array))->asArray($row, $context);
        $skipKeys = (new Parameter($this->skipKeys))->asArray($row, $context);
        $entryPrefix = (new Parameter($this->entryPrefix))->asString($row, $context);

        if ($array === null || $skipKeys === null) {
            throw new InvalidArgumentException('ArrayUnpack requires non-null array and skipKeys');
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
