<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Double\Api;

final class Step extends BaseStep
{
    public function next(): Pipeline
    {
        return new Pipeline();
    }
}
