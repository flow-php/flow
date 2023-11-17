<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Money\Currencies\ISOCurrencies;
use Money\Currency;
use Money\Money;
use Money\MoneyParser;
use Money\Parser\DecimalMoneyParser;

if (!\interface_exists(\Money\MoneyParser::class)) {
    throw new RuntimeException("Money\MoneyParser class not found, please add moneyphp/money dependency to the project first.");
}

final class ToMoney implements ScalarFunction
{
    public function __construct(
        private readonly ScalarFunction $amountRef,
        private readonly ScalarFunction $currencyRef,
        private readonly MoneyParser $moneyParser = new DecimalMoneyParser(new ISOCurrencies())
    ) {
    }

    public function eval(Row $row) : ?Money
    {
        $currency = $this->currencyRef->eval($row);

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
