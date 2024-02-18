<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS;

use Flow\ETL\Filesystem\Path;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Rows;

final class ChartJSLoader implements Closure, Loader
{
    public function __construct(
        private readonly Chart $type,
        private readonly ?Path $output = null,
        private readonly Path $template = new Path(__DIR__ . '/Resources/template/full_page.html'),
        private ?array &$outputVar = null
    ) {
    }

    public function closure(FlowContext $context) : void
    {
        if ($this->output === null && $this->outputVar === null) {
            return;
        }

        if ($this->output !== null) {
            if ($context->streams()->exists($this->output)) {
                $context->streams()->rm($this->output);
            }

            $output = $context->streams()->writeTo($this->output);

            $templateStream = $context->streams()->read($this->template);

            /** @var string $template */
            $template = \stream_get_contents($templateStream->resource());
            $templateStream->close();

            $content = \str_replace(
                '%_CHART_DATA_%',
                \json_encode($this->type->data(), JSON_THROW_ON_ERROR),
                $template
            );

            \fwrite($output->resource(), $content);

            $context->streams()->closeWriters($this->output);
        }

        if ($this->outputVar !== null) {
            $this->outputVar = $this->type->data();
        }
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$rows->count()) {
            return;
        }

        $this->type->collect($rows);
    }
}
