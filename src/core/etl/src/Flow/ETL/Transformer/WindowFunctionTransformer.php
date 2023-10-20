<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Window;

/**
 * @implements Transformer<array{entry_name: string, window: Window}>
 */
final class WindowFunctionTransformer implements Transformer
{
    public function __construct(
        private readonly string $entryName,
        private readonly Window $window,
    ) {
    }

    public function __serialize() : array
    {
        return [
            'entry_name' => $this->entryName,
            'window' => $this->window,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entryName = $data['entry_name'];
        $this->window = $data['window'];
    }

    /**
     * @throws InvalidArgumentException
     * @throws \JsonException
     * @throws RuntimeException
     */
    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $newRows = new Rows();

        foreach ($rows as $row) {
            $newRows = $newRows->add(
                $row->add($context->entryFactory()->create($this->entryName, $this->window->function()->apply($row, $rows, $this->window)))
            );
        }

        return $newRows;
    }
}
