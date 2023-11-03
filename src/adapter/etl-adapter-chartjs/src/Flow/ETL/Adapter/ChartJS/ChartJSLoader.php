<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS;

use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{type: Chart, output: ?Path, template: Path}>
 */
final class ChartJSLoader implements Closure, Loader
{
    public function __construct(
        private readonly Chart $type,
        private readonly ?Path $output = null,
        private readonly Path $template = new Path(__DIR__ . '/Resources/template/full_page.html')
    ) {
    }

    public function __serialize() : array
    {
        return [
            'type' => $this->type,
            'output' => $this->output,
            'template' => $this->template,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->type = $data['type'];
        $this->output = $data['output'];
        $this->template = $data['template'];
    }

    public function closure(FlowContext $context) : void
    {
        if ($this->output === null) {
            return;
        }

        if ($context->streams()->exists($this->output)) {
            $context->streams()->rm($this->output);
        }

        $stream = $context->streams()->open($this->output, 'html', Mode::WRITE, false);

        $templateStream = $context->streams()->fs()->open($this->template, Mode::READ);

        /** @var string $template */
        $template = \stream_get_contents($context->streams()->fs()->open($this->template, Mode::READ)->resource());
        $templateStream->close();

        $output = \str_replace(
            '%_CHART_DATA_%',
            \json_encode($this->type->data(), JSON_THROW_ON_ERROR),
            $template
        );

        \fwrite($stream->resource(), $output);
        $stream->close();
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$rows->count()) {
            return;
        }

        $this->type->collect($rows);
    }
}
