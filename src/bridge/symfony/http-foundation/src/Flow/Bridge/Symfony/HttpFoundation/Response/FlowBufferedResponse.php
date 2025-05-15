<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Response;

use function Flow\ETL\DSL\{df};
use function Flow\Filesystem\DSL\{path_memory, protocol};
use Flow\Bridge\Symfony\HttpFoundation\Output;
use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\{Config, Extractor, Transformation, Transformations};
use Symfony\Component\HttpFoundation\Response;

final class FlowBufferedResponse extends Response
{
    private bool $buffered = false;

    private readonly Config|ConfigBuilder $config;

    public function __construct(
        private readonly Extractor $extractor,
        private readonly Output $output,
        private readonly Transformation $transformations = new Transformations(),
        int $status = 200,
        array $headers = [],
        Config|ConfigBuilder|null $config = null,
    ) {
        $this->config = $config ?? Config::default();

        parent::__construct(null, $status, $headers);
    }

    public function getContent() : string
    {
        $this->evaluate();

        return $this->content;
    }

    public function sendContent() : static
    {
        $this->evaluate();

        print $this->content;

        return $this;
    }

    private function evaluate() : void
    {
        if ($this->buffered) {
            return;
        }

        $config = $this->config instanceof ConfigBuilder ? $this->config->build() : $this->config;

        df($config)
            ->read($this->extractor)
            ->with($this->transformations)
            ->dropPartitions()
            ->write($this->output->memoryLoader($id = \bin2hex(\random_bytes(16)) . '.memory'))
            ->run();

        $fs = $config->fstab()->for(protocol('memory'));

        if ($fs->status(path_memory($id)) === null) {
            $this->buffered = true;
            $this->content = '';
            $this->statusCode = self::HTTP_NO_CONTENT;

            return;
        }

        $this->content = $fs->readFrom(path_memory($id))->content();
        $this->buffered = true;
    }
}
