<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

use Flow\PgQuery\Protobuf\AST\{DefElem, DoStmt, Node, PBString};
use Flow\PgQuery\QueryBuilder\AstToSql;

final readonly class DoBuilder implements DoFinalStep
{
    use AstToSql;

    private function __construct(
        private string $code,
        private string $language = 'plpgsql',
    ) {
    }

    public static function create(string $code) : DoFinalStep
    {
        return new self($code);
    }

    public function language(string $language) : DoFinalStep
    {
        return new self(
            $this->code,
            $language,
        );
    }

    public function toAst() : DoStmt
    {
        $stmt = new DoStmt();

        $args = [];

        $asDefElem = new DefElem();
        $asDefElem->setDefname('as');

        $codeStr = new PBString();
        $codeStr->setSval($this->code);
        $codeNode = new Node();
        $codeNode->setString($codeStr);
        $asDefElem->setArg($codeNode);

        $asNode = new Node();
        $asNode->setDefElem($asDefElem);
        $args[] = $asNode;

        $langDefElem = new DefElem();
        $langDefElem->setDefname('language');

        $langStr = new PBString();
        $langStr->setSval($this->language);
        $langNode = new Node();
        $langNode->setString($langStr);
        $langDefElem->setArg($langNode);

        $langNodeWrapper = new Node();
        $langNodeWrapper->setDefElem($langDefElem);
        $args[] = $langNodeWrapper;

        $stmt->setArgs($args);

        return $stmt;
    }
}
