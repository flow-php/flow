<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use function Flow\ETL\DSL\{ref, row, str_entry};
use Flow\ETL\Row\Reference;
use Flow\ETL\{FlowContext, Row, Rows, Transformer};

final readonly class SerializeTransformer implements Transformer
{
    public function __construct(private Reference|string $target, private bool $standalone = false)
    {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $target = $this->target instanceof Reference ? $this->target : ref($this->target);

        return $rows->map(
            fn (Row $row) => $this->standalone
                ? row(str_entry($target->name(), $context->config->serializer()->serialize($row)))
                : $row->add(str_entry($target->name(), $context->config->serializer()->serialize($row)))
        );
    }
}
