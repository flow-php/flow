<?php

declare(strict_types=1);

namespace Flow\Tools\Rector;

use PhpParser\Node\{FunctionLike, Param};
use PhpParser\{Node};
use Rector\BetterPhpDocParser\PhpDocInfo\{PhpDocInfoFactory};
use Rector\BetterPhpDocParser\PhpDocManipulator\PhpDocTypeChanger;
use Rector\Rector\AbstractRector;
use Symplify\RuleDocGenerator\ValueObject\RuleDefinition;

final class AddArrayParamDocTypeMixed extends AbstractRector
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
        $this->processParams($node);

        return null;
    }

    private function processParams(FunctionLike $functionLike) : void
    {
        foreach ($functionLike->getParams() as $param) {
            if (!$param->type) {
                continue;
            }

            $type = $param->type;

            if (!$this->isArrayType($type)) {
                continue;
            }

            $phpDocInfo = $this->phpDocInfoFactory->createFromNodeOrEmpty($functionLike);

            $paramTagValueNode = $phpDocInfo->getParamTagValueByName($this->getName($param));

            if ($paramTagValueNode !== null) {
                continue;
            }

            $paramName = $this->getName($param);

            $newType = $this->resolveType($type);
            $this->phpDocTypeChanger->changeParamType(
                $functionLike,
                $phpDocInfo,
                $newType,
                $param,
                $paramName
            );
        }
    }
}
