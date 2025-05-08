<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Pipeline;

trait RecursivePipelineIterator
{
    /**
     * @return array<Pipeline>
     */
    private function allPipelines(Pipeline $pipeline) : array
    {
        $pipelines = [];

        if ($pipeline instanceof OverridingPipeline) {
            $pipelines[] = $pipeline;

            foreach ($pipeline->pipelines() as $nextPipeline) {
                $pipelines = [...$pipelines, ...$this->allPipelines($nextPipeline)];
            }
        } else {
            $pipelines[] = $pipeline;
        }

        return $pipelines;
    }
}
