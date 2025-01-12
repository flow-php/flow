<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use function Flow\ETL\DSL\ref;
use Flow\ETL\Row\Reference;
use Flow\ETL\{FlowContext, Row, Rows, Transformer};
use Flow\Serializer\Exception\SerializationException;

final readonly class UnserializeTransformer implements Transformer
{
    /**
     * @param Reference|string $source
     * @param bool $merge
     * @param string $mergePrefix - used only when merge is set to true
     */
    public function __construct(private Reference|string $source, private bool $merge = true, private string $mergePrefix = '')
    {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $source = $this->source instanceof Reference ? $this->source : ref($this->source);

        return $rows->map(
            function (Row $row) use ($source, $context) : Row {
                if (!$row->has($source->name())) {
                    return $row;
                }

                $serialized = $row->valueOf($source->name());

                if (!\is_string($serialized)) {
                    return $row;
                }

                try {
                    return $this->merge
                        ? $row->merge($context->config->serializer()->unserialize($serialized, [Row::class]), $this->mergePrefix)
                        : $context->config->serializer()->unserialize($serialized, [Row::class]);
                } catch (SerializationException) {
                    return $row;
                }
            }
        );
    }
}
