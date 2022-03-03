<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Row;
use Flow\ETL\Row\Schema\Definition;
use Flow\Serializer\Serializable;

final class Schema implements Serializable
{
    /**
     * @var array<Definition>
     */
    private array $definitions;

    public function __construct(Definition ...$definitions)
    {
        $this->definitions = $definitions;
    }

    /**
     * @return array{definitions: array<Definition>}
     */
    public function __serialize() : array
    {
        return [
            'definitions' => $this->definitions,
        ];
    }

    /**
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @param array{definitions: array<Definition>} $data
     */
    public function __unserialize(array $data) : void
    {
        $this->definitions = $data['definitions'];
    }

    public function isValid(Row $row) : bool
    {
        if (\count($this->definitions) !== $row->entries()->count()) {
            return false;
        }

        foreach ($row->entries()->all() as $entry) {
            $isValid = false;

            foreach ($this->definitions as $definition) {
                if ($definition->matches($entry)) {
                    $isValid = true;

                    break;
                }
            }

            if (!$isValid) {
                return false;
            }
        }

        return true;
    }
}
