<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Row, Rows, Schema\Definition, Transformer};
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunction\{ExpandResults, ScalarResult, UnpackResults};

final readonly class ScalarFunctionTransformer implements Transformer
{
    public function __construct(
        private string|Definition $entry,
        public ScalarFunction $function,
    ) {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        if ($this->function instanceof ExpandResults) {
            return $rows->flatMap(
                fn (Row $r) : array => \array_map(
                    fn ($val) : Row => new Row(
                        $r->entries()
                            ->set(
                                $context->entryFactory()->create($this->entryName(), $val, $this->entry instanceof Definition ? $this->entry : null)
                            )
                    ),
                    $this->function->eval($r)
                )
            );
        }

        if ($this->function instanceof UnpackResults) {
            return $rows->map(
                function (Row $r) use ($context) : Row {
                    /**
                     * @var array-key $key
                     * @var mixed $val
                     */
                    foreach ($this->function->eval($r) as $key => $val) {
                        $r = $r->set($context->entryFactory()->create($this->entryName() . '.' . $key, $val));
                    }

                    return $r;
                }
            );
        }

        return $rows->map(
            function (Row $r) use ($context) : Row {
                $value = $this->function->eval($r);
                $type = $this->entry instanceof Definition ? $this->entry->type() : null;

                if ($value instanceof ScalarResult) {
                    $type = $value->type;
                    $value = $value->value;
                }

                return $r->set(
                    $type
                        ? $context->entryFactory()->createAs($this->entryName(), $value, $type)
                        : $context->entryFactory()->create($this->entryName(), $value, $this->entry instanceof Definition ? $this->entry : null)
                );
            }
        );
    }

    private function entryName() : string
    {
        return $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry;
    }
}
