<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunction\ExpandResults;
use Flow\ETL\{FlowContext, Row, Row\Schema\Definition, Rows, Transformer};

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
                                $context->entryFactory()->create(
                                    $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry,
                                    $val,
                                    $this->entry instanceof Definition ? $this->entry : null
                                )
                            )
                    ),
                    (array) $this->function->eval($r)
                )
            );
        }

        return $rows->map(
            function (Row $r) use ($context) : Row {
                /** @var mixed $value */
                $value = $this->function->eval($r);

                if ($this->function instanceof ScalarFunction\UnpackResults) {
                    /**
                     * @var array-key $key
                     * @var mixed $val
                     */
                    foreach ($value as $key => $val) {
                        $r = $r->set($context->entryFactory()->create(($this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry) . '.' . $key, $val));
                    }

                    return $r;
                }

                $val = $this->function->eval($r);

                return $r->set(
                    $this->function instanceof ScalarFunction\TypedScalarFunction
                     ? $context->entryFactory()->createAs(
                         $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry,
                         $val,
                         $this->function->returns()
                     )
                     : $context->entryFactory()->create(
                         $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry,
                         $val,
                         $this->entry instanceof Definition ? $this->entry : null
                     )
                );
            }
        );
    }
}
