<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{entry: string, expression: Expression, entry_factory: EntryFactory}>
 */
final class EntryExpressionEvalTransformer implements Transformer
{
    public function __construct(
        private readonly string $entryName,
        private readonly Expression $expression,
        private readonly EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
    }

    public function __serialize() : array
    {
        return [
            'entry' => $this->entryName,
            'expression' => $this->expression,
            'entry_factory' => $this->entryFactory,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entryName = $data['entry'];
        $this->expression = $data['expression'];
        $this->entryFactory = $data['entry_factory'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        if ($this->expression instanceof Row\Reference\ExpandResults && $this->expression->expand()) {
            return $rows->flatMap(
                function (Row $r) : array {
                    return \array_map(
                        fn ($val) : Row => new Row(
                            $r->entries()
                                ->merge(new Entries($this->entryFactory->create($this->entryName, $val)))
                        ),
                        (array) $this->expression->eval($r)
                    );
                }
            );
        }

        return $rows->map(
            function (Row $r) : Row {
                /** @var mixed $value */
                $value = $this->expression->eval($r);

                if (\is_array($value)) {
                    if ($this->expression instanceof Row\Reference\ExplodeResults && $this->expression->explode()) {
                        /**
                         * @var array-key $key
                         * @var mixed $val
                         */
                        foreach ($value as $key => $val) {
                            $r = $r->set($this->entryFactory->create($this->entryName . '.' . $key, $val));
                        }

                        return $r;
                    }
                }

                return $r->set($this->entryFactory->create($this->entryName, $this->expression->eval($r)));
            }
        );
    }
}
