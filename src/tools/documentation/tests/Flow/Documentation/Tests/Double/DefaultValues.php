<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Double;

final class DefaultValues
{
    public function refused(RefusedDefault $value = new RefusedDefault()): void {}

    public function required(int $value): void {}

    public function scalar(int $value = 5): void {}
}
