<?php

declare(strict_types=1);

namespace Flow\Tools\Rector;

use PhpParser\Node;
use PhpParser\Node\{FunctionLike};
use Rector\BetterPhpDocParser\PhpDocInfo\PhpDocInfoFactory;
use Rector\BetterPhpDocParser\PhpDocManipulator\PhpDocTypeChanger;
use Rector\Rector\AbstractRector;
use Symplify\RuleDocGenerator\ValueObject\RuleDefinition;

final class AddArrayReturnDocTypeMixed extends AbstractRector
{
    use ArrayDocBlockType;

    public function __construct(
        private PhpDocInfoFactory $phpDocInfoFactory,
        private PhpDocTypeChanger $phpDocTypeChanger,
    ) {
    }

    /**
     * @return array<string>
     */
    public function getNodeTypes() : array
    {
        return [FunctionLike::class];
    }

    public function getRuleDefinition() : RuleDefinition
    {
        return new RuleDefinition('', []);
    }

    /**
     * @param FunctionLike $node
     */
    public function refactor(Node $node) : ?Node
    {

        $this->processReturn($node);

        return null;
    }

    private function processReturn(FunctionLike $functionLike) : void
    {
        $type = $functionLike->getReturnType();

        if (!$type) {
            return;
        }

        if (!$this->isArrayType($type)) {
            return;
        }

        $phpDocInfo = $this->phpDocInfoFactory->createFromNodeOrEmpty($functionLike);

        if ($phpDocInfo->hasByName('return')) {
            return;
        }

        $newType = $this->resolveType($type);
        $this->phpDocTypeChanger->changeReturnType($functionLike, $phpDocInfo, $newType);
    }
}
