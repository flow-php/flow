<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunction\ExpandResults;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{entry: string, function: ScalarFunction}>
 */
final class ScalarFunctionTransformer implements Transformer
{
    public function __construct(
        private readonly string $entryName,
        public readonly ScalarFunction $function,
    ) {
    }

    public function __serialize() : array
    {
        return [
            'entry' => $this->entryName,
            'function' => $this->function,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entryName = $data['entry'];
        $this->function = $data['function'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        if ($this->function instanceof ExpandResults && $this->function->expand()) {
            return $rows->flatMap(
                fn (Row $r) : array => \array_map(
                    fn ($val) : Row => new Row(
                        $r->entries()
                            ->merge(new Entries($context->entryFactory()->create($this->entryName, $val, $context->schema())))
                    ),
                    (array) $this->function->eval($r)
                )
            );
        }

        return $rows->map(
            function (Row $r) use ($context) : Row {
                /** @var mixed $value */
                $value = $this->function->eval($r);

                if (\is_array($value)) {
                    if ($this->function instanceof ScalarFunction\UnpackResults && $this->function->unpack()) {
                        /**
                         * @var array-key $key
                         * @var mixed $val
                         */
                        foreach ($value as $key => $val) {
                            $r = $r->set($context->entryFactory()->create($this->entryName . '.' . $key, $val, $context->schema()));
                        }

                        return $r;
                    }
                }

                return $r->set($context->entryFactory()->create($this->entryName, $this->function->eval($r), $context->schema()));
            }
        );
    }
}
