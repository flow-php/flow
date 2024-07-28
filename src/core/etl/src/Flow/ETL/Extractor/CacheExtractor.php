<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\{Extractor, FlowContext, Rows};

final class CacheExtractor implements Extractor
{
    public function __construct(
        private readonly string $id,
        private readonly ?Extractor $fallbackExtractor = null,
        private readonly bool $clear = false
    ) {
    }

    /**
     * @param FlowContext $context
     *
     * @return \Generator<int, Rows, mixed, void>
     */
    public function extract(FlowContext $context) : \Generator
    {
        if (!$context->rowsCache()->has($this->id)) {
            if ($this->fallbackExtractor !== null) {
                foreach ($this->fallbackExtractor->extract($context) as $rows) {
                    $signal = yield $rows;

                    if ($signal === Signal::STOP) {
                        return;
                    }
                }
            }
        } else {
            foreach ($context->rowsCache()->get($this->id) as $rows) {
                $signal = yield $rows;

                if ($signal === Signal::STOP) {
                    return;
                }
            }
        }

        if ($this->clear) {
            $context->rowsCache()->remove($this->id);
        }
    }
}
