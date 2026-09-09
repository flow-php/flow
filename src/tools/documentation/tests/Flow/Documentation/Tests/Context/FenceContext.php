<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Context;

use Flow\Documentation\Docs\DefinedFunctions;
use Flow\Documentation\Docs\DocumentDeclarations;
use Flow\Documentation\Docs\Fence;
use Flow\Documentation\Docs\FenceInfo;
use Flow\Documentation\Docs\FenceSymbols;

final readonly class FenceContext
{
    public function php(string $code, string $info = 'php'): Fence
    {
        return new Fence('a.md', 1, FenceInfo::parse($info), $code);
    }

    /**
     * @return list<string>
     */
    public function unresolvedFunctions(string $code, string ...$declaredByThePage): array
    {
        $declarations = [];

        foreach ($declaredByThePage as $declaration) {
            $declarations[] = $this->php($declaration);
        }

        return (new FenceSymbols())->unresolved(
            $this->php($code),
            new DefinedFunctions(),
            new DocumentDeclarations($declarations),
        );
    }
}
