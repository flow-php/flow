<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\type_string;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Row;
use Money\Currencies\ISOCurrencies;
use Money\Parser\DecimalMoneyParser;
use Money\{Currency, Money, MoneyParser};

if (!\interface_exists(MoneyParser::class)) {
    throw new RuntimeException("Money\MoneyParser class not found, please add moneyphp/money dependency to the project first.");
}

final class ToMoney extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $amountRef,
        private readonly ScalarFunction $currencyRef,
        private readonly MoneyParser $moneyParser = new DecimalMoneyParser(new ISOCurrencies())
    ) {
    }

    public function eval(Row $row) : ?Money
    {
        $currency = Caster::default()->to(type_string(true))->value($this->currencyRef->eval($row));

        if (!\is_string($currency)) {
            return null;
        }

        if ('' === $currency) {
            return null;
        }

        $amount = $this->amountRef->eval($row);

        if (!\is_numeric($amount)) {
            return null;
        }

        return $this->moneyParser->parse((string) $amount, new Currency($currency));
    }
}
