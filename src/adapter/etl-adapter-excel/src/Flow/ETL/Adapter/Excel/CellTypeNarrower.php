<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Schema\Inference\InferredTypes;
use Flow\Types\Type;
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use Flow\Types\Type\TypeNarrower;

use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function is_string;

final readonly class CellTypeNarrower implements TypeNarrower
{
    private InstanceOfTypeNarrower $cells;

    private ?StringTypeNarrower $text;

    public function __construct(InferredTypes $candidates)
    {
        $this->cells = new InstanceOfTypeNarrower();

        $emits = [];

        // a workbook has a cell type for numbers, booleans and dates, so text that looks like one is text by
        // choice; it has none for these, so text is the only way to write them and the only way to read them back
        foreach ([type_json(), type_uuid(), type_time_zone()] as $type) {
            if ($candidates->allows($type)) {
                $emits[] = $type;
            }
        }

        $this->text = $emits === [] ? null : new StringTypeNarrower($emits);
    }

    /**
     * @return Type<mixed>
     */
    public function narrow(mixed $value): Type
    {
        if ($this->text !== null && is_string($value)) {
            return $this->text->narrow($value);
        }

        return $this->cells->narrow($value);
    }
}
